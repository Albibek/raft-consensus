use crate::raft::*;
use crate::*;
use raft_consensus::*;

#[test]
fn test_kickoff() {
    // Test the very first stage of init: election of a leader
    // after all nodes have started as followers
    let mut cluster = TestCluster::new(3);
    for node in cluster.nodes.values() {
        assert_eq!(node.kind(), ConsensusState::Follower);
    }
    cluster.apply_actions();
    cluster.asset_leader_condition();
}

//#[test]
//// test a most probable green scenario: a 3-node cluster
//// with everything in order
//fn test_typical_green() {
//let mut cluster = TestCluster::new(3);

//}
