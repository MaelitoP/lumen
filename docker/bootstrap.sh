#!/bin/sh
# Forms the three-node Raft cluster once every node's HTTP API is up. Idempotent:
# if a leader already exists (e.g. on a re-up over persisted volumes), it exits.
set -eu

N1="http://node1:7700"

for h in node1 node2 node3; do
    echo "waiting for $h..."
    until curl -fsS "http://$h:7700/health" >/dev/null 2>&1; do sleep 1; done
done

for _ in 1 2 3 4 5; do
    if curl -fsS "$N1/cluster/metrics" 2>/dev/null | grep -q '"current_leader":[1-9]'; then
        echo "cluster already initialized:"
        curl -fsS "$N1/cluster/metrics"
        echo
        exit 0
    fi
    sleep 1
done

echo "initializing node1..."
curl -fsS -X POST "$N1/cluster/init" \
    -H 'content-type: application/json' \
    -d '{"members":[{"node_id":1,"rpc_addr":"node1:8080"}]}'

echo "waiting for node1 to lead..."
until curl -fsS "$N1/cluster/metrics" | grep -q '"current_leader":1'; do sleep 1; done

echo "adding learners..."
curl -fsS -X POST "$N1/cluster/learners" \
    -H 'content-type: application/json' \
    -d '{"node_id":2,"rpc_addr":"node2:8080"}'
curl -fsS -X POST "$N1/cluster/learners" \
    -H 'content-type: application/json' \
    -d '{"node_id":3,"rpc_addr":"node3:8080"}'

echo "promoting to a 3-voter cluster..."
curl -fsS -X POST "$N1/cluster/membership" \
    -H 'content-type: application/json' \
    -d '{"members":[1,2,3]}'

echo "cluster formed:"
curl -fsS "$N1/cluster/metrics"
echo
