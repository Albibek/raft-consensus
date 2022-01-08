use raft_consensus::testing::emulation::cluster::*;
use raft_consensus::testing::emulation::handler::*;

use raft_consensus::message::admin::*;
use raft_consensus::*;

/// Tests where node should generate errors

#[test]
fn test_leader_transfer_error() {
    // Test the very first stage of init: election of a leader
    // after all nodes have started as followers
    let mut cluster = TestCluster::new(3, false);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();
    let admin_id = AdminId(uuid::Uuid::from_slice(&[0u8; 16]).unwrap());
    cluster.apply_action(Action::Admin(
        admin_id,
        ServerId(0),
        AdminMessage::StepDownRequest(Some(ServerId(10))),
    ));

    cluster.apply_peer_packets();
    let response = cluster
        .handler
        .admin_network
        .get_mut(&(ServerId(0), admin_id))
        .unwrap()
        .pop_front()
        .unwrap();
    assert_ne!(
        response,
        AdminMessage::StepDownResponse(ConfigurationChangeResponse::Started)
    );
    assert_ne!(
        response,
        AdminMessage::StepDownResponse(ConfigurationChangeResponse::Success)
    );
    // ensure node 0 is leader, because of wrong id
    for (id, node) in &cluster.nodes {
        if id == &ServerId(0) {
            assert_eq!(node.kind(), ConsensusState::Leader);
        } else {
            assert_eq!(node.kind(), ConsensusState::Follower);
        }
    }
}
