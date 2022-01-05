use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use crate::handler::*;
use crate::raft::*;
use crate::*;

use bytes::Bytes;
use log::trace;

use raft_consensus::message::*;
use raft_consensus::*;

/// Tests for green scenarios(when network works OK, and nodes do not fail)

// TODO: test stale leader (leader with lower term than rest of the cluster)
// TODO: test solitary leader (1-node cluster)

#[test]
fn test_kickstart() {
    // Test the very first stage of init: election of a leader
    // after all nodes have started as followers
    let mut cluster = TestCluster::new(3, false);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();
    // assertions are made inside kickstart
    for _ in 0..50 {
        cluster.apply_heartbeats();
        // clear election tiemouts
        for (_, t) in &mut cluster.handler.election_timeouts {
            *t = false
        }
        cluster.apply_peer_packets();
        // ensure all of them were called to be reset while ping messages keep coming
        for (id, t) in &mut cluster.handler.election_timeouts {
            // leader does not set the election timeout
            if id != &ServerId(0) {
                assert_eq!(*t, true);
            }
        }
        // ensure node 0 stays leader
        for (id, node) in &cluster.nodes {
            if id == &ServerId(0) {
                assert_eq!(node.kind(), ConsensusState::Leader);
            } else {
                assert_eq!(node.kind(), ConsensusState::Follower);
            }
        }
    }
}

#[test]
fn test_sticky_leader() {
    // Test the very first stage of init: election of a leader
    // after all nodes have started as followers
    let mut cluster = TestCluster::new(3, false);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();
    // assetrions are made inside kickstart

    // sticky leader should deal with this situation
    // the voting requests shold be sent and delayed, but
    // after applying heartbeats they have to be ignored and leader must stay
    // intact
    cluster.apply_action(Action::Timeout(ServerId(1), Timeout::Election));
    cluster.apply_peer_packets();

    for _ in 0..50 {
        cluster.apply_heartbeats();
        // clear election timeouts

        cluster.apply_peer_packets();

        // ensure node 0 stays leader
        for (id, node) in &cluster.nodes {
            if id == &ServerId(0) {
                assert_eq!(node.kind(), ConsensusState::Leader);
            } else {
                assert_eq!(node.kind(), ConsensusState::Follower);
            }
        }
    }
}

#[test]
fn test_leader_transfer_auto() {
    // Test the leader transfer where any follower node can become a leader
    let mut cluster = TestCluster::new(3, false);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();
    let admin_id = AdminId(uuid::Uuid::from_slice(&[0u8; 16]).unwrap());
    cluster.apply_action(Action::Admin(
        admin_id,
        ServerId(0),
        AdminMessage::StepDownRequest(None),
    ));

    cluster.apply_peer_packets();
    let response = cluster
        .handler
        .admin_network
        .get_mut(&(ServerId(0), admin_id))
        .unwrap()
        .pop_front()
        .unwrap();

    assert_eq!(
        response,
        AdminMessage::StepDownResponse(ConfigurationChangeResponse::Started)
    );

    trace!("states: {:?}", cluster.nodes.values().collect::<Vec<_>>());
    // ensure node 1 or 2 is leader now (due to usage of hashmap in leader, they may differ
    // from test to test because of being on the same log index
    assert_eq!(
        cluster.nodes.get(&ServerId(0)).unwrap().kind(),
        ConsensusState::Follower
    );

    let state1 = cluster.nodes.get(&ServerId(1)).unwrap().kind();
    let state2 = cluster.nodes.get(&ServerId(2)).unwrap().kind();
    assert!(
        state1 == ConsensusState::Follower && state2 == ConsensusState::Leader
            || state1 == ConsensusState::Leader && state2 == ConsensusState::Follower
    );
}

#[test]
fn test_leader_transfer_manual() {
    // Test the leader transfer when leader ID is specified explicitly
    let mut cluster = TestCluster::new(3, false);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();
    let admin_id = AdminId(uuid::Uuid::from_slice(&[0u8; 16]).unwrap());
    cluster.apply_action(Action::Admin(
        admin_id,
        ServerId(0),
        AdminMessage::StepDownRequest(Some(ServerId(2))),
    ));

    cluster.apply_peer_packets();
    // ensure node 2 is leader now
    for (id, node) in &cluster.nodes {
        if id == &ServerId(2) {
            assert_eq!(node.kind(), ConsensusState::Leader);
        } else {
            assert_eq!(node.kind(), ConsensusState::Follower);
        }
    }
}

#[test]
fn test_client_proposal() {
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
            guarantee: ClientGuarantee::default(),
        }),
    ));
    // send the proposal to followers (after timeout, because of default client guarantee)
    // and process their responses
    cluster.apply_heartbeats();
    cluster.apply_peer_packets();

    let responses = cluster
        .handler
        .client_network
        .get(&(leader_id, client_id))
        .unwrap();

    dbg!(responses);

    // TODO: apply client packets, make sure the hash is good there

    // ping the followers so they could know the the proposal is committed
    cluster.apply_heartbeats();
    cluster.apply_peer_packets();

    // ensure the hash is the same on all the state machines and in the response
    let mut hasher = DefaultHasher::new();
    for byte in &query {
        hasher.write_u8(*byte);
    }

    let expected_hash = hasher.finish();

    for (id, node) in &cluster.nodes {
        dbg!(id, node.state_machine().unwrap().hash, expected_hash);
        assert_eq!(node.state_machine().unwrap().hash, expected_hash);
    }
}
