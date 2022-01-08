use raft_consensus::testing::emulation::cluster::*;

use raft_consensus::message::*;
use raft_consensus::*;

// TODO: test configuration change on solitary leader (1-node cluster)

#[test]
fn test_add_server() {
    let mut cluster = TestCluster::new(3, false);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.kickstart();

    let admin_id = AdminId(uuid::Uuid::from_slice(&[42u8; 16]).unwrap());
    let leader_id = ServerId(0);
}
