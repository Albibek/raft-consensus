use raft_consensus::testing::emulation::cluster::*;

use raft_consensus::message::*;
use raft_consensus::*;

// TODO: test configuration change on solitary leader (1-node cluster)

// TODO: adding existing peer, removing non-existing peer

#[test]
fn test_add_server_with_connected_node() {
    let mut cluster = TestCluster::new(3);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();

    let admin_id = AdminId(uuid::Uuid::from_slice(&[42u8; 16]).unwrap());
    let leader_id = ServerId(0);
    let new_node_id = ServerId(3);

    cluster.add_admin(admin_id);

    // connect a new node
    cluster.add_node(new_node_id);

    let command = AdminMessage::AddServerRequest(AddServerRequest {
        id: ServerId(3),
        info: vec![0, 0, 3],
    });

    // request leader to add new node
    cluster.apply_admin_request(admin_id, leader_id, command);

    cluster.apply_peer_packets();
}
