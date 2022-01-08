use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use raft_consensus::testing::emulation::cluster::*;
use raft_consensus::testing::emulation::handler::*;

use bytes::Bytes;

use raft_consensus::message::*;
use raft_consensus::*;

/// Tests for snapshotting mechanisms

// TODO: test snapshots for solitary leader (1-node cluster)
//
// TODO: test partially applying snapshots, for example: follower's snapshot is at LogIndex(2),
// but leader sends entries LogIndex(1)..LogIndex(5). LogIndex(1) and LogIndex(2)  have to be
// skipped in this case
//
// TODO: test taking snapshot on the leader while follower snapshot is still applied

#[test]
fn test_local_compaction() {
    let mut cluster = TestCluster::new(3, false);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();
    // LogIndex = 1 because of empty entry after voting)
    let client_id = ClientId(uuid::Uuid::from_slice(&[0u8; 16]).unwrap());
    let leader_id = ServerId(0);

    // client proposal will be inserted at LogIndex = 2
    let query = Bytes::from((&[0, 0, 0, 42]).as_slice());
    cluster.apply_action(Action::Client(
        client_id,
        leader_id,
        ClientMessage::ClientProposalRequest(ClientRequest {
            data: query.clone(),
            guarantee: ClientGuarantee::Fast,
        }),
    ));

    let mut hasher = DefaultHasher::new();
    for byte in &query {
        hasher.write_u8(*byte);
    }

    let expected_hash = hasher.finish();

    // fast apply the proposal
    cluster.apply_peer_packets();

    // try to snapshot a follower id=1
    // (we expect the state machine to do the snapshotting)
    let res = cluster.check_compaction(&[ServerId(1)], true);
    assert!(res[0]);

    for id in cluster.nodes.keys() {
        let machine = cluster.machine_of(id.clone());
        assert_eq!(machine.state, expected_hash);
    }
}

#[test]
fn test_snapshot_transfer_on_follower_failure() {
    let mut cluster = TestCluster::new(3, false);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();
    // LogIndex = 1 because of empty entry after voting)
    let client_id = ClientId(uuid::Uuid::from_slice(&[0u8; 16]).unwrap());
    let leader_id = ServerId(0);

    // client proposal will be inserted at LogIndex = 2
    let query = Bytes::from((&[0, 0, 0, 42]).as_slice());
    cluster.apply_action(Action::Client(
        client_id,
        leader_id,
        ClientMessage::ClientProposalRequest(ClientRequest {
            data: query.clone(),
            guarantee: ClientGuarantee::Fast,
        }),
    ));

    let mut hasher = DefaultHasher::new();
    for byte in &query {
        hasher.write_u8(*byte);
    }

    let expected_hash = hasher.finish();

    // fast replicate the entry
    cluster.apply_peer_packets();

    // snapshot the leader: entry at LogIndex(2) should go into the snapshot
    let res = cluster.check_compaction(&[leader_id], true);
    assert!(res[0]);

    // emulate follower fail by recreating the one from scratch
    // false is the same as at the beginning: disables chunking at state machine
    cluster.add_node(ServerId(2), false);

    // now apply the timeouts and make sure leader will send the snapshot to ServerId(2)
    cluster.apply_heartbeats();
    cluster.apply_peer_packets();

    //    cluster.apply_heartbeats();

    // try to snapshot a follower id=1

    for id in cluster.nodes.keys() {
        let machine = cluster.machine_of(id.clone());
        assert_eq!(machine.state, expected_hash);
    }
}
#[test]
fn test_snapshot_transfer_chunked() {
    let mut cluster = TestCluster::new(3, true);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();
    // LogIndex = 1 because of empty entry after voting)
    let client_id = ClientId(uuid::Uuid::from_slice(&[0u8; 16]).unwrap());
    let leader_id = ServerId(0);

    // client proposal will be inserted at LogIndex = 2
    let query = Bytes::from((&[0, 0, 0, 42]).as_slice());
    cluster.apply_action(Action::Client(
        client_id,
        leader_id,
        ClientMessage::ClientProposalRequest(ClientRequest {
            data: query.clone(),
            guarantee: ClientGuarantee::Log,
        }),
    ));

    let mut hasher = DefaultHasher::new();
    for byte in &query {
        hasher.write_u8(*byte);
    }

    let expected_hash = hasher.finish();

    // Fire the timeout only for the first follower
    // This must trigger the commit of LogIndex(2)
    cluster.apply_action(Action::Timeout(
        ServerId(0),
        Timeout::Heartbeat(ServerId(1)),
    ));

    cluster.apply_peer_packets();

    // now snapshot the leader: entry at LogIndex(2) should go into the snapshot
    // so sending the log to follower at ServerId(2) will not be possible
    // ans the snapshot will be sent instead

    // (we expect the state machine to do the snapshotting)
    let res = cluster.check_compaction(&[leader_id], true);
    assert!(res[0]);

    // now apply the timeout and make sure leader will send the snapshot to follower id = 2
    cluster.apply_heartbeats();
    cluster.apply_peer_packets();
    cluster.apply_heartbeats();
    cluster.apply_peer_packets();

    // try to snapshot a follower id=1
    for id in cluster.nodes.keys() {
        let machine = cluster.machine_of(id.clone());
        assert_eq!(machine.state, expected_hash);
    }
}
