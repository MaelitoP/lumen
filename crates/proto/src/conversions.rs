use std::collections::BTreeSet;
use std::fmt;

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{
    Entry, EntryPayload, LeaderId, LogId, Membership, RaftTypeConfig, SnapshotMeta,
    StoredMembership, Vote,
};

use crate::raft;
use crate::v1::Command;

#[derive(Debug)]
pub enum ConversionError {
    MissingField(&'static str),
}

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConversionError::MissingField(field) => write!(f, "missing required field `{field}`"),
        }
    }
}

impl std::error::Error for ConversionError {}

fn require<T>(opt: Option<T>, field: &'static str) -> Result<T, ConversionError> {
    opt.ok_or(ConversionError::MissingField(field))
}

impl From<LeaderId<u64>> for raft::LeaderId {
    fn from(v: LeaderId<u64>) -> Self {
        raft::LeaderId {
            term: v.term,
            node_id: v.node_id,
        }
    }
}

impl From<raft::LeaderId> for LeaderId<u64> {
    fn from(v: raft::LeaderId) -> Self {
        LeaderId::new(v.term, v.node_id)
    }
}

impl From<Vote<u64>> for raft::Vote {
    fn from(v: Vote<u64>) -> Self {
        raft::Vote {
            leader_id: Some(v.leader_id.into()),
            committed: v.committed,
        }
    }
}

impl TryFrom<raft::Vote> for Vote<u64> {
    type Error = ConversionError;
    fn try_from(v: raft::Vote) -> Result<Self, Self::Error> {
        Ok(Vote {
            leader_id: require(v.leader_id, "Vote.leader_id")?.into(),
            committed: v.committed,
        })
    }
}

impl From<LogId<u64>> for raft::LogId {
    fn from(v: LogId<u64>) -> Self {
        raft::LogId {
            leader_id: Some(v.leader_id.into()),
            index: v.index,
        }
    }
}

impl TryFrom<raft::LogId> for LogId<u64> {
    type Error = ConversionError;
    fn try_from(v: raft::LogId) -> Result<Self, Self::Error> {
        Ok(LogId::new(
            require(v.leader_id, "LogId.leader_id")?.into(),
            v.index,
        ))
    }
}

impl From<Membership<u64, raft::Node>> for raft::Membership {
    fn from(m: Membership<u64, raft::Node>) -> Self {
        raft::Membership {
            configs: m
                .get_joint_config()
                .iter()
                .map(|set| raft::NodeIdSet {
                    node_ids: set.iter().copied().collect(),
                })
                .collect(),
            nodes: m.nodes().map(|(id, n)| (*id, n.clone())).collect(),
        }
    }
}

impl TryFrom<raft::Membership> for Membership<u64, raft::Node> {
    type Error = ConversionError;
    fn try_from(m: raft::Membership) -> Result<Self, Self::Error> {
        let configs: Vec<BTreeSet<u64>> = m
            .configs
            .into_iter()
            .map(|s| s.node_ids.into_iter().collect())
            .collect();
        Ok(Membership::new(configs, m.nodes))
    }
}

impl From<StoredMembership<u64, raft::Node>> for raft::StoredMembership {
    fn from(s: StoredMembership<u64, raft::Node>) -> Self {
        raft::StoredMembership {
            log_id: s.log_id().map(Into::into),
            membership: Some(s.membership().clone().into()),
        }
    }
}

impl TryFrom<raft::StoredMembership> for StoredMembership<u64, raft::Node> {
    type Error = ConversionError;
    fn try_from(s: raft::StoredMembership) -> Result<Self, Self::Error> {
        let log_id = s.log_id.map(TryInto::try_into).transpose()?;
        let membership = require(s.membership, "StoredMembership.membership")?.try_into()?;
        Ok(StoredMembership::new(log_id, membership))
    }
}

impl From<SnapshotMeta<u64, raft::Node>> for raft::SnapshotMeta {
    fn from(m: SnapshotMeta<u64, raft::Node>) -> Self {
        raft::SnapshotMeta {
            last_log_id: m.last_log_id.map(Into::into),
            last_membership: Some(m.last_membership.into()),
            snapshot_id: m.snapshot_id,
        }
    }
}

impl TryFrom<raft::SnapshotMeta> for SnapshotMeta<u64, raft::Node> {
    type Error = ConversionError;
    fn try_from(m: raft::SnapshotMeta) -> Result<Self, Self::Error> {
        Ok(SnapshotMeta {
            last_log_id: m.last_log_id.map(TryInto::try_into).transpose()?,
            last_membership: require(m.last_membership, "SnapshotMeta.last_membership")?
                .try_into()?,
            snapshot_id: m.snapshot_id,
        })
    }
}

impl<C> From<Entry<C>> for raft::Entry
where
    C: RaftTypeConfig<NodeId = u64, Node = raft::Node, D = Command>,
{
    fn from(e: Entry<C>) -> Self {
        use raft::entry::Payload;
        let payload = match e.payload {
            EntryPayload::Blank => Payload::Blank(raft::Unit {}),
            EntryPayload::Normal(cmd) => Payload::Normal(cmd),
            EntryPayload::Membership(m) => Payload::Membership(m.into()),
        };
        raft::Entry {
            log_id: Some(e.log_id.into()),
            payload: Some(payload),
        }
    }
}

impl<C> TryFrom<raft::Entry> for Entry<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = raft::Node, D = Command>,
{
    type Error = ConversionError;
    fn try_from(e: raft::Entry) -> Result<Self, Self::Error> {
        use raft::entry::Payload;
        let log_id = require(e.log_id, "Entry.log_id")?.try_into()?;
        let payload = match require(e.payload, "Entry.payload")? {
            Payload::Blank(_) => EntryPayload::Blank,
            Payload::Normal(cmd) => EntryPayload::Normal(cmd),
            Payload::Membership(m) => EntryPayload::Membership(m.try_into()?),
        };
        Ok(Entry { log_id, payload })
    }
}

impl From<VoteRequest<u64>> for raft::VoteRequest {
    fn from(r: VoteRequest<u64>) -> Self {
        raft::VoteRequest {
            vote: Some(r.vote.into()),
            last_log_id: r.last_log_id.map(Into::into),
        }
    }
}

impl TryFrom<raft::VoteRequest> for VoteRequest<u64> {
    type Error = ConversionError;
    fn try_from(r: raft::VoteRequest) -> Result<Self, Self::Error> {
        Ok(VoteRequest {
            vote: require(r.vote, "VoteRequest.vote")?.try_into()?,
            last_log_id: r.last_log_id.map(TryInto::try_into).transpose()?,
        })
    }
}

impl From<VoteResponse<u64>> for raft::VoteResponse {
    fn from(r: VoteResponse<u64>) -> Self {
        raft::VoteResponse {
            vote: Some(r.vote.into()),
            vote_granted: r.vote_granted,
            last_log_id: r.last_log_id.map(Into::into),
        }
    }
}

impl TryFrom<raft::VoteResponse> for VoteResponse<u64> {
    type Error = ConversionError;
    fn try_from(r: raft::VoteResponse) -> Result<Self, Self::Error> {
        Ok(VoteResponse {
            vote: require(r.vote, "VoteResponse.vote")?.try_into()?,
            vote_granted: r.vote_granted,
            last_log_id: r.last_log_id.map(TryInto::try_into).transpose()?,
        })
    }
}

impl<C> From<AppendEntriesRequest<C>> for raft::AppendEntriesRequest
where
    C: RaftTypeConfig<NodeId = u64, Node = raft::Node, D = Command, Entry = Entry<C>>,
{
    fn from(r: AppendEntriesRequest<C>) -> Self {
        raft::AppendEntriesRequest {
            vote: Some(r.vote.into()),
            prev_log_id: r.prev_log_id.map(Into::into),
            entries: r.entries.into_iter().map(Into::into).collect(),
            leader_commit: r.leader_commit.map(Into::into),
        }
    }
}

impl<C> TryFrom<raft::AppendEntriesRequest> for AppendEntriesRequest<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = raft::Node, D = Command, Entry = Entry<C>>,
{
    type Error = ConversionError;
    fn try_from(r: raft::AppendEntriesRequest) -> Result<Self, Self::Error> {
        let entries = r
            .entries
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(AppendEntriesRequest {
            vote: require(r.vote, "AppendEntriesRequest.vote")?.try_into()?,
            prev_log_id: r.prev_log_id.map(TryInto::try_into).transpose()?,
            entries,
            leader_commit: r.leader_commit.map(TryInto::try_into).transpose()?,
        })
    }
}

impl From<AppendEntriesResponse<u64>> for raft::AppendEntriesResponse {
    fn from(r: AppendEntriesResponse<u64>) -> Self {
        use raft::append_entries_response::Result as R;
        let result = match r {
            AppendEntriesResponse::Success => R::Success(raft::Unit {}),
            AppendEntriesResponse::PartialSuccess(m) => R::PartialSuccess(raft::PartialSuccess {
                matching: m.map(Into::into),
            }),
            AppendEntriesResponse::Conflict => R::Conflict(raft::Unit {}),
            AppendEntriesResponse::HigherVote(v) => R::HigherVote(v.into()),
        };
        raft::AppendEntriesResponse {
            result: Some(result),
        }
    }
}

impl TryFrom<raft::AppendEntriesResponse> for AppendEntriesResponse<u64> {
    type Error = ConversionError;
    fn try_from(r: raft::AppendEntriesResponse) -> Result<Self, Self::Error> {
        use raft::append_entries_response::Result as R;
        Ok(match require(r.result, "AppendEntriesResponse.result")? {
            R::Success(_) => AppendEntriesResponse::Success,
            R::PartialSuccess(p) => AppendEntriesResponse::PartialSuccess(
                p.matching.map(TryInto::try_into).transpose()?,
            ),
            R::Conflict(_) => AppendEntriesResponse::Conflict,
            R::HigherVote(v) => AppendEntriesResponse::HigherVote(v.try_into()?),
        })
    }
}

impl From<SnapshotResponse<u64>> for raft::SnapshotResponse {
    fn from(r: SnapshotResponse<u64>) -> Self {
        raft::SnapshotResponse {
            vote: Some(r.vote.into()),
        }
    }
}

impl TryFrom<raft::SnapshotResponse> for SnapshotResponse<u64> {
    type Error = ConversionError;
    fn try_from(r: raft::SnapshotResponse) -> Result<Self, Self::Error> {
        Ok(SnapshotResponse {
            vote: require(r.vote, "SnapshotResponse.vote")?.try_into()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Cursor;

    openraft::declare_raft_types!(
        pub TC:
            D = Command,
            R = (),
            NodeId = u64,
            Node = crate::raft::Node,
    );

    fn node(id: u64) -> raft::Node {
        raft::Node {
            node_id: id,
            rpc_addr: format!("127.0.0.1:{id}"),
        }
    }

    fn vote() -> Vote<u64> {
        Vote {
            leader_id: LeaderId::new(3, 7),
            committed: true,
        }
    }

    fn log_id(index: u64) -> LogId<u64> {
        LogId::new(LeaderId::new(2, 5), index)
    }

    fn joint_membership() -> Membership<u64, raft::Node> {
        let mut nodes = BTreeMap::new();
        nodes.insert(1, node(1));
        nodes.insert(2, node(2));
        nodes.insert(3, node(3));
        nodes.insert(4, node(4));
        let configs = vec![BTreeSet::from([1, 2]), BTreeSet::from([2, 3])];
        Membership::new(configs, nodes)
    }

    #[test]
    fn vote_round_trips() {
        for committed in [false, true] {
            let original = Vote {
                leader_id: LeaderId::new(9, 1),
                committed,
            };
            let proto: raft::Vote = original.into();
            let back: Vote<u64> = proto.try_into().expect("vote");
            assert_eq!(original, back);
        }
    }

    #[test]
    fn log_id_round_trips() {
        let original = log_id(42);
        let proto: raft::LogId = original.into();
        let back: LogId<u64> = proto.try_into().expect("log id");
        assert_eq!(original, back);
    }

    #[test]
    fn joint_membership_with_learner_round_trips() {
        let original = joint_membership();
        let proto: raft::Membership = original.clone().into();
        let back: Membership<u64, raft::Node> = proto.try_into().expect("membership");
        assert_eq!(original, back);
    }

    #[test]
    fn stored_membership_round_trips() {
        for log in [None, Some(log_id(11))] {
            let original = StoredMembership::new(log, joint_membership());
            let proto: raft::StoredMembership = original.clone().into();
            let back: StoredMembership<u64, raft::Node> =
                proto.try_into().expect("stored membership");
            assert_eq!(original, back);
        }
    }

    #[test]
    fn snapshot_meta_round_trips() {
        for last in [None, Some(log_id(99))] {
            let original = SnapshotMeta::<u64, raft::Node> {
                last_log_id: last,
                last_membership: StoredMembership::new(Some(log_id(7)), joint_membership()),
                snapshot_id: "snap-1".to_string(),
            };
            let proto: raft::SnapshotMeta = original.clone().into();
            let back: SnapshotMeta<u64, raft::Node> = proto.try_into().expect("snapshot meta");
            assert_eq!(original, back);
        }
    }

    #[test]
    fn entry_round_trips_all_payloads() {
        let payloads = [
            EntryPayload::<TC>::Blank,
            EntryPayload::Normal(Command { op: None }),
            EntryPayload::Membership(joint_membership()),
        ];
        for payload in payloads {
            let original = Entry::<TC> {
                log_id: log_id(5),
                payload,
            };
            let proto: raft::Entry = original.clone().into();
            let back: Entry<TC> = proto.try_into().expect("entry");
            assert_eq!(original, back);
        }
    }

    #[test]
    fn vote_request_response_round_trip() {
        let req = VoteRequest::new(vote(), Some(log_id(3)));
        let proto: raft::VoteRequest = req.clone().into();
        let back: VoteRequest<u64> = proto.try_into().expect("vote request");
        assert_eq!(req, back);

        let resp = VoteResponse::new(vote(), Some(log_id(4)), true);
        let proto: raft::VoteResponse = resp.clone().into();
        let back: VoteResponse<u64> = proto.try_into().expect("vote response");
        assert_eq!(resp, back);
    }

    #[test]
    fn append_entries_request_round_trips() {
        let req = AppendEntriesRequest::<TC> {
            vote: vote(),
            prev_log_id: Some(log_id(1)),
            entries: vec![
                Entry {
                    log_id: log_id(2),
                    payload: EntryPayload::Blank,
                },
                Entry {
                    log_id: log_id(3),
                    payload: EntryPayload::Normal(Command { op: None }),
                },
            ],
            leader_commit: Some(log_id(2)),
        };
        let proto: raft::AppendEntriesRequest = req.clone().into();
        let back: AppendEntriesRequest<TC> = proto.try_into().expect("append entries request");
        assert_eq!(req.vote, back.vote);
        assert_eq!(req.prev_log_id, back.prev_log_id);
        assert_eq!(req.leader_commit, back.leader_commit);
        assert_eq!(req.entries, back.entries);
    }

    #[test]
    fn append_entries_heartbeat_round_trips() {
        let req = AppendEntriesRequest::<TC> {
            vote: vote(),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let proto: raft::AppendEntriesRequest = req.clone().into();
        let back: AppendEntriesRequest<TC> = proto.try_into().expect("append entries request");
        assert_eq!(req.vote, back.vote);
        assert_eq!(req.prev_log_id, back.prev_log_id);
        assert_eq!(req.leader_commit, back.leader_commit);
        assert_eq!(req.entries, back.entries);
    }

    #[test]
    fn append_entries_response_round_trips() {
        let cases: [fn() -> AppendEntriesResponse<u64>; 5] = [
            || AppendEntriesResponse::Success,
            || AppendEntriesResponse::PartialSuccess(None),
            || AppendEntriesResponse::PartialSuccess(Some(log_id(8))),
            || AppendEntriesResponse::Conflict,
            || AppendEntriesResponse::HigherVote(vote()),
        ];
        for case in cases {
            let proto: raft::AppendEntriesResponse = case().into();
            let back: AppendEntriesResponse<u64> =
                proto.try_into().expect("append entries response");
            assert_eq!(case(), back);
        }
    }

    #[test]
    fn snapshot_response_round_trips() {
        let proto: raft::SnapshotResponse = SnapshotResponse::new(vote()).into();
        let back: SnapshotResponse<u64> = proto.try_into().expect("snapshot response");
        assert_eq!(SnapshotResponse::new(vote()), back);
    }

    #[test]
    fn missing_field_is_reported() {
        let err = Vote::<u64>::try_from(raft::Vote {
            leader_id: None,
            committed: false,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            ConversionError::MissingField("Vote.leader_id")
        ));
    }

    #[test]
    fn missing_entry_payload_is_reported() {
        let err = Entry::<TC>::try_from(raft::Entry {
            log_id: Some(log_id(1).into()),
            payload: None,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            ConversionError::MissingField("Entry.payload")
        ));
    }

    #[test]
    fn missing_append_entries_result_is_reported() {
        let err =
            AppendEntriesResponse::<u64>::try_from(raft::AppendEntriesResponse { result: None })
                .unwrap_err();
        assert!(matches!(
            err,
            ConversionError::MissingField("AppendEntriesResponse.result")
        ));
    }
}
