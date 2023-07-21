package org.apache.helix.metaclient.recipes.leaderelection;

import java.util.ConcurrentModificationException;
import org.apache.helix.metaclient.MetaClientTestUtil;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.impl.zk.ZkMetaClient;
import org.apache.helix.metaclient.impl.zk.ZkMetaClientTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.helix.metaclient.impl.zk.TestUtil.*;


public class TestLeaderElection extends ZkMetaClientTestBase {

  private static final String PARTICIPANT_NAME1 = "participant_1";
  private static final String PARTICIPANT_NAME2 = "participant_2";
  private static final String LEADER_PATH = "/LEADER_ELECTION_GROUP_1";

  public LeaderElectionClient createLeaderElectionClient(String participantName) {
    MetaClientConfig.StoreType storeType = MetaClientConfig.StoreType.ZOOKEEPER;
    MetaClientConfig config =
        new MetaClientConfig.MetaClientConfigBuilder<>().setConnectionAddress(ZK_ADDR).setStoreType(storeType).build();
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
    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH) != null);
    }, MetaClientTestUtil.WAIT_DURATION));
    Assert.assertNotNull(clt1.getLeader(LEADER_PATH));
    Assert.assertEquals(clt1.getLeader(LEADER_PATH), clt2.getLeader(LEADER_PATH));
    Assert.assertEquals(clt1.getLeader(LEADER_PATH), PARTICIPANT_NAME1);

    // client 1 exit leader election group, and client 2 should be current leader.
    clt1.exitLeaderElectionParticipantPool(LEADER_PATH);
    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH) != null);
    }, MetaClientTestUtil.WAIT_DURATION));
    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH).equals(PARTICIPANT_NAME2));
    }, MetaClientTestUtil.WAIT_DURATION));

    // client1 join and client2 leave. client 1 should be leader.
    clt1.joinLeaderElectionParticipantPool(LEADER_PATH);
    clt2.exitLeaderElectionParticipantPool(LEADER_PATH);
    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH) != null);
    }, MetaClientTestUtil.WAIT_DURATION));
    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (clt1.getLeader(LEADER_PATH).equals(PARTICIPANT_NAME1));
    }, MetaClientTestUtil.WAIT_DURATION));
    Assert.assertTrue(clt1.isLeader(LEADER_PATH));
    Assert.assertFalse(clt2.isLeader(LEADER_PATH));

    clt1.close();
    clt2.close();
  }

  @Test
  public void testElectionPoolMembership() throws Exception {
    String leaderPath = LEADER_PATH + "/testElectionPoolMembership";
    LeaderInfo participantInfo = new LeaderInfo(PARTICIPANT_NAME1);
    participantInfo.setSimpleField("Key1", "value1");
    LeaderInfo participantInfo2 = new LeaderInfo(PARTICIPANT_NAME2);
    participantInfo2.setSimpleField("Key2", "value2");
    LeaderElectionClient clt1 = createLeaderElectionClient(PARTICIPANT_NAME1);
    LeaderElectionClient clt2 = createLeaderElectionClient(PARTICIPANT_NAME2);

    clt1.joinLeaderElectionParticipantPool(leaderPath, participantInfo);
    try {
      clt1.joinLeaderElectionParticipantPool(leaderPath, participantInfo); // no op
    } catch (ConcurrentModificationException ex) {
      // expected
      Assert.assertEquals(ex.getClass().getName(), "java.util.ConcurrentModificationException");
    }
    clt2.joinLeaderElectionParticipantPool(leaderPath, participantInfo2);

    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (clt1.getLeader(leaderPath) != null);
    }, MetaClientTestUtil.WAIT_DURATION));
    Assert.assertNotNull(clt1.getLeaderEntryStat(leaderPath));
    Assert.assertNotNull(clt1.getLeader(leaderPath));
    Assert.assertEquals(clt1.getParticipantInfo(leaderPath, PARTICIPANT_NAME1).getSimpleField("Key1"), "value1");
    Assert.assertEquals(clt2.getParticipantInfo(leaderPath, PARTICIPANT_NAME1).getSimpleField("Key1"), "value1");
    Assert.assertEquals(clt1.getParticipantInfo(leaderPath, PARTICIPANT_NAME2).getSimpleField("Key2"), "value2");
    Assert.assertEquals(clt2.getParticipantInfo(leaderPath, PARTICIPANT_NAME2).getSimpleField("Key2"), "value2");

    // clt1 gone
    clt1.relinquishLeader(leaderPath);
    clt1.exitLeaderElectionParticipantPool(leaderPath);
    clt2.exitLeaderElectionParticipantPool(leaderPath);

    Assert.assertNull(clt2.getParticipantInfo(LEADER_PATH, PARTICIPANT_NAME2));
  }

  @Test
  public void testSessionExpire() throws Exception {
    String leaderPath = LEADER_PATH + "/testSessionExpire";
    LeaderInfo participantInfo = new LeaderInfo(PARTICIPANT_NAME1);
    participantInfo.setSimpleField("Key1", "value1");
    LeaderInfo participantInfo2 = new LeaderInfo(PARTICIPANT_NAME2);
    participantInfo2.setSimpleField("Key2", "value2");
    LeaderElectionClient clt1 = createLeaderElectionClient(PARTICIPANT_NAME1);
    LeaderElectionClient clt2 = createLeaderElectionClient(PARTICIPANT_NAME2);

    clt1.joinLeaderElectionParticipantPool(leaderPath, participantInfo);
    try {
      clt1.joinLeaderElectionParticipantPool(leaderPath, participantInfo); // no op
    } catch (ConcurrentModificationException ex) {
      // expected
      Assert.assertEquals(ex.getClass().getName(), "java.util.ConcurrentModificationException");
    }
    clt2.joinLeaderElectionParticipantPool(leaderPath, participantInfo2);
    // a leader should be up
    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (clt1.getLeader(leaderPath) != null);
    }, MetaClientTestUtil.WAIT_DURATION));

    // session expire and reconnect
    expireSession((ZkMetaClient) clt1.getMetaClient());

    Assert.assertTrue(MetaClientTestUtil.verify(() -> {
      return (clt1.getLeader(leaderPath) != null);
    }, MetaClientTestUtil.WAIT_DURATION));
    Assert.assertNotNull(clt1.getLeaderEntryStat(leaderPath));
    Assert.assertNotNull(clt1.getLeader(leaderPath));
    // when session recreated, participant info node should maintain
    Assert.assertEquals(clt1.getParticipantInfo(leaderPath, PARTICIPANT_NAME1).getSimpleField("Key1"), "value1");
    Assert.assertEquals(clt2.getParticipantInfo(leaderPath, PARTICIPANT_NAME1).getSimpleField("Key1"), "value1");
    Assert.assertEquals(clt1.getParticipantInfo(leaderPath, PARTICIPANT_NAME2).getSimpleField("Key2"), "value2");
    Assert.assertEquals(clt2.getParticipantInfo(leaderPath, PARTICIPANT_NAME2).getSimpleField("Key2"), "value2");
  }
}
