package org.apache.helix.metaclient.recipes.leaderelection;

import org.apache.helix.metaclient.TestUtil;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.impl.zk.ZkMetaClientTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestLeaderElection extends ZkMetaClientTestBase {

  private static final String PARTICIPANT_NAME1 = "participant_1";
  private static final String PARTICIPANT_NAME2 = "participant_2";
  private static final String LEADER_PATH = "/LEADER_ELECTION_GROUP_1";

  public LeaderElectionClient createLeaderElectionClient(String participantName) {
    MetaClientConfig.StoreType storeType = MetaClientConfig.StoreType.ZOOKEEPER;
    MetaClientConfig config = new MetaClientConfig.MetaClientConfigBuilder<>().setConnectionAddress(ZK_ADDR)
        .setStoreType(storeType).build();
    return new LeaderElectionClient(config, participantName);
  }

  @Test
  public void testAcquireLeadership() throws Exception {
    // create 2 clients representing 2 participants
    LeaderElectionClient clt1 = createLeaderElectionClient(PARTICIPANT_NAME1);
    LeaderElectionClient clt2 = createLeaderElectionClient(PARTICIPANT_NAME2);

    clt1.joinLeaderElectionParticipantPool(LEADER_PATH);
    clt2.joinLeaderElectionParticipantPool(LEADER_PATH);
    // First client joining the leader election group should be current leader
    Assert.assertTrue(TestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH) != null);
    }, TestUtil.WAIT_DURATION));
    Assert.assertNotNull(clt1.getLeader(LEADER_PATH));
    Assert.assertEquals(clt1.getLeader(LEADER_PATH), clt2.getLeader(LEADER_PATH));
    Assert.assertEquals(clt1.getLeader(LEADER_PATH), PARTICIPANT_NAME1);

    // client 1 exit leader election group, and client 2 should be current leader.
    clt1.exitLeaderElectionParticipantPool(LEADER_PATH);
    Assert.assertTrue(TestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH) != null);
    }, TestUtil.WAIT_DURATION));
    Assert.assertTrue(TestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH).equals(PARTICIPANT_NAME2));
    }, TestUtil.WAIT_DURATION));

    // client1 join and client2 leave. client 1 should be leader.
    clt1.joinLeaderElectionParticipantPool(LEADER_PATH);
    clt2.exitLeaderElectionParticipantPool(LEADER_PATH);
    Assert.assertTrue(TestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH) != null);
    }, TestUtil.WAIT_DURATION));
    Assert.assertTrue(TestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH).equals(PARTICIPANT_NAME1));
    }, TestUtil.WAIT_DURATION));
    Assert.assertTrue(clt1.isLeader(LEADER_PATH));
    Assert.assertFalse(clt2.isLeader(LEADER_PATH));

    clt1.close();
    clt2.close();
  }

}
