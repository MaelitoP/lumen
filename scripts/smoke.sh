#!/usr/bin/env bash
# Smoke test for the Lumen HTTP API.
#
# Starts a real server, runs a few CRUD/search requests, kills it, and checks
# that data is still there after restart. The short checkpoint interval keeps
# the test fast.
set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${LUMEN_SMOKE_PORT:-7799}"
ADDR="127.0.0.1:${PORT}"
BASE="http://${ADDR}"
DATA="$(mktemp -d)"
LOG="${DATA}/server.log"
BIN="target/debug/lumen"
SERVER_PID=""

if [[ "$(uname)" == "Darwin" ]]; then
  export LIBRARY_PATH="$(brew --prefix libiconv)/lib"
fi

cleanup() {
  [[ -n "${SERVER_PID}" ]] && kill "${SERVER_PID}" 2>/dev/null || true
  rm -rf "${DATA}"
}
trap cleanup EXIT

start_server() {
  "${BIN}" --data-dir "${DATA}" --bind "${ADDR}" --checkpoint-interval-secs 1 \
    >>"${LOG}" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 100); do
    if curl -fsS "${BASE}/health" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      echo "server exited during startup; log:" >&2
      cat "${LOG}" >&2
      exit 1
    fi
    sleep 0.1
  done

  echo "server did not become ready" >&2
  cat "${LOG}" >&2
  exit 1
}

crash_server() {
  kill -9 "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  SERVER_PID=""
}

stop_server_graceful() {
  kill -TERM "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  SERVER_PID=""
}

req() {
  local method="$1" path="$2" body="${3:-}"

  echo "+ ${method} ${path}${body:+  ${body}}"
  if [[ -n "${body}" ]]; then
    curl -sS -X "${method}" "${BASE}${path}" -H 'content-type: application/json' \
      -d "${body}" -w $'\n-> HTTP %{http_code}\n'
  else
    curl -sS -X "${method}" "${BASE}${path}" -w $'\n-> HTTP %{http_code}\n'
  fi
  echo
}

wait_searchable() {
  local path="$1"

  for _ in $(seq 1 50); do
    if curl -fsS "${BASE}${path}" | grep -q '"total":[1-9]'; then
      req GET "${path}"
      return 0
    fi
    sleep 0.1
  done

  echo "documents never became searchable at ${path}" >&2
  exit 1
}

echo "== build =="
cargo build -p lumen-api >/dev/null
echo "data dir: ${DATA}"
echo

echo "== start server =="
start_server
echo "ready at ${BASE}"
echo

echo "== collections =="
req PUT  /collections/books '{"fields":{"title":{"type":"text","indexed":true},"year":{"type":"i64","indexed":true,"fast":true}}}'
req GET  /collections
req GET  /collections/books

echo "== index documents =="
req POST /collections/books/documents '{"title":"The Rust Programming Language","year":2018}'
req PUT  /collections/books/documents/tdg '{"title":"Designing Data-Intensive Applications","year":2017}'

echo "== search =="
wait_searchable '/collections/books/documents/search?q=rust'
req GET  /collections/books/documents/tdg

echo "== crash and restart =="
crash_server
start_server
echo "-- checking recovered data"
wait_searchable '/collections/books/documents/search?q=data'

echo "== delete + idempotent re-create + error paths =="
req DELETE /collections/books/documents/tdg
req PUT    /collections/books '{"fields":{"title":{"type":"text","indexed":true},"year":{"type":"i64","indexed":true,"fast":true}}}'
req PUT    /collections/books '{"fields":{"title":{"type":"keyword","indexed":true}}}'
req GET    /collections/nope

stop_server_graceful
