use std::future::Future;
use std::io::SeekFrom;
use std::sync::{Arc, Mutex};

use futures::Stream;
use lumen_proto::raft::raft_service_client::RaftServiceClient;
use lumen_proto::raft::raft_service_server::{RaftService, RaftServiceServer};
use lumen_proto::raft::{self, SnapshotRequest};
use lumen_proto::ConversionError;
use openraft::error::{
    Fatal, InstallSnapshotError, NetworkError, RPCError, RaftError, ReplicationClosed,
    StreamingError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{
    AnyError, OptionalSend, Snapshot, SnapshotMeta, StorageError, StorageIOError, Vote,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Response, Status, Streaming};

use crate::type_config::{Node, NodeId, TypeConfig};
use crate::LumenRaft;

const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Clone, Default, Debug)]
pub struct NetworkFactory;

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Client;

    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        Client {
            node: node.clone(),
            channel: None,
        }
    }
}

#[derive(Debug)]
pub struct Client {
    node: Node,
    channel: Option<Channel>,
}

impl Client {
    async fn channel(&mut self) -> Result<Channel, Unreachable> {
        if let Some(channel) = &self.channel {
            return Ok(channel.clone());
        }
        let addr = &self.node.rpc_addr;
        let endpoint =
            Endpoint::from_shared(format!("http://{addr}")).map_err(|e| Unreachable::new(&e))?;
        let channel = endpoint.connect().await.map_err(|e| Unreachable::new(&e))?;
        self.channel = Some(channel.clone());
        Ok(channel)
    }

    fn invalidate_on_transport(&mut self, status: &Status) -> bool {
        if is_transport(status) {
            self.channel = None;
            true
        } else {
            false
        }
    }

    fn rpc_status<E: std::error::Error + 'static>(
        &mut self,
        status: Status,
    ) -> RPCError<NodeId, Node, E> {
        if self.invalidate_on_transport(&status) {
            RPCError::Unreachable(Unreachable::new(&status))
        } else {
            RPCError::Network(NetworkError::new(&status))
        }
    }
}

impl RaftNetwork<TypeConfig> for Client {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let channel = self.channel().await.map_err(RPCError::Unreachable)?;
        let mut request = Request::new(rpc.into());
        request.set_timeout(option.hard_ttl());
        let response = RaftServiceClient::new(channel)
            .append_entries(request)
            .await
            .map_err(|s| self.rpc_status(s))?;
        response
            .into_inner()
            .try_into()
            .map_err(|e| RPCError::Network(conv_err(e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let channel = self.channel().await.map_err(RPCError::Unreachable)?;
        let mut request = Request::new(rpc.into());
        request.set_timeout(option.hard_ttl());
        let response = RaftServiceClient::new(channel)
            .vote(request)
            .await
            .map_err(|s| self.rpc_status(s))?;
        response
            .into_inner()
            .try_into()
            .map_err(|e| RPCError::Network(conv_err(e)))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(Unreachable::new(&UnusedRpc)))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfig, Fatal<NodeId>>> {
        let channel = self.channel().await?;

        let chunk_size = option
            .snapshot_chunk_size()
            .unwrap_or(DEFAULT_CHUNK_SIZE)
            .max(1);
        let proto_vote: raft::Vote = vote.into();
        let proto_meta: raft::SnapshotMeta = snapshot.meta.into();

        let mut file = *snapshot.snapshot;
        file.seek(SeekFrom::Start(0)).await.map_err(read_snapshot)?;

        let read_err = Arc::new(Mutex::new(None));
        let stream = chunk_stream(
            file,
            proto_vote,
            proto_meta,
            chunk_size,
            Arc::clone(&read_err),
        );

        let mut client = RaftServiceClient::new(channel);
        tokio::pin!(cancel);
        let outcome = tokio::select! {
            biased;
            reason = &mut cancel => return Err(StreamingError::Closed(reason)),
            res = client.snapshot(stream) => res,
        };

        if let Some(e) = read_err
            .lock()
            .expect("snapshot read-error slot poisoned")
            .take()
        {
            return Err(StreamingError::StorageError(read_snapshot(e)));
        }

        match outcome {
            Ok(response) => response
                .into_inner()
                .try_into()
                .map_err(|e| StreamingError::Network(conv_err(e))),
            Err(status) => Err(if self.invalidate_on_transport(&status) {
                StreamingError::Unreachable(Unreachable::new(&status))
            } else {
                StreamingError::Network(NetworkError::new(&status))
            }),
        }
    }
}

fn chunk_stream(
    file: tokio::fs::File,
    vote: raft::Vote,
    meta: raft::SnapshotMeta,
    chunk_size: usize,
    read_err: Arc<Mutex<Option<std::io::Error>>>,
) -> impl Stream<Item = SnapshotRequest> + Send + 'static {
    futures::stream::unfold(file, move |mut file| {
        let meta = meta.clone();
        let read_err = Arc::clone(&read_err);
        async move {
            let mut buf = vec![0u8; chunk_size];
            match file.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    buf.truncate(n);
                    let request = SnapshotRequest {
                        vote: Some(vote),
                        meta: Some(meta),
                        data: buf,
                    };
                    Some((request, file))
                }
                Err(e) => {
                    *read_err.lock().expect("snapshot read-error slot poisoned") = Some(e);
                    None
                }
            }
        }
    })
}

#[derive(Clone)]
pub struct RaftServer {
    raft: LumenRaft,
}

impl std::fmt::Debug for RaftServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftServer").finish_non_exhaustive()
    }
}

impl RaftServer {
    pub fn new(raft: LumenRaft) -> Self {
        Self { raft }
    }

    pub fn into_service(self) -> RaftServiceServer<Self> {
        RaftServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn vote(
        &self,
        request: Request<raft::VoteRequest>,
    ) -> Result<Response<raft::VoteResponse>, Status> {
        let rpc: VoteRequest<NodeId> = request.into_inner().try_into().map_err(invalid_arg)?;
        let response = self.raft.vote(rpc).await.map_err(internal)?;
        Ok(Response::new(response.into()))
    }

    async fn append_entries(
        &self,
        request: Request<raft::AppendEntriesRequest>,
    ) -> Result<Response<raft::AppendEntriesResponse>, Status> {
        let rpc: AppendEntriesRequest<TypeConfig> =
            request.into_inner().try_into().map_err(invalid_arg)?;
        let response = self.raft.append_entries(rpc).await.map_err(internal)?;
        Ok(Response::new(response.into()))
    }

    async fn snapshot(
        &self,
        request: Request<Streaming<SnapshotRequest>>,
    ) -> Result<Response<raft::SnapshotResponse>, Status> {
        let mut stream = request.into_inner();
        let mut data = self
            .raft
            .begin_receiving_snapshot()
            .await
            .map_err(internal)?;
        let mut vote: Option<Vote<NodeId>> = None;
        let mut meta: Option<SnapshotMeta<NodeId, Node>> = None;

        while let Some(chunk) = stream.message().await? {
            if vote.is_none() {
                vote = Some(
                    require(chunk.vote, "SnapshotRequest.vote")?
                        .try_into()
                        .map_err(invalid_arg)?,
                );
                meta = Some(
                    require(chunk.meta, "SnapshotRequest.meta")?
                        .try_into()
                        .map_err(invalid_arg)?,
                );
            }
            data.write_all(&chunk.data).await.map_err(internal)?;
        }
        data.flush().await.map_err(internal)?;
        data.sync_all().await.map_err(internal)?;

        let vote = vote.ok_or_else(|| Status::invalid_argument("empty snapshot stream"))?;
        let meta = meta.ok_or_else(|| Status::invalid_argument("empty snapshot stream"))?;
        let snapshot = Snapshot {
            meta,
            snapshot: data,
        };
        let response = self
            .raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(internal)?;
        Ok(Response::new(response.into()))
    }
}

fn is_transport(status: &Status) -> bool {
    matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
}

fn conv_err(e: ConversionError) -> NetworkError {
    NetworkError::new(&e)
}

fn read_snapshot(e: std::io::Error) -> StorageError<NodeId> {
    StorageIOError::read_snapshot(None, AnyError::new(&e)).into()
}

fn require<T>(opt: Option<T>, field: &'static str) -> Result<T, Status> {
    opt.ok_or_else(|| Status::invalid_argument(format!("missing required field `{field}`")))
}

fn invalid_arg<E: std::fmt::Display>(e: E) -> Status {
    Status::invalid_argument(e.to_string())
}

fn internal<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(e.to_string())
}

#[derive(Debug)]
struct UnusedRpc;

impl std::fmt::Display for UnusedRpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("install_snapshot is unused; full_snapshot is overridden for client-streaming")
    }
}

impl std::error::Error for UnusedRpc {}
