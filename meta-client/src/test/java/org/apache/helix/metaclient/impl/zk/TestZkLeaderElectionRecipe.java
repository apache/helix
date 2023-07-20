package org.apache.helix.metaclient.impl.zk;

import java.util.ConcurrentModificationException;
import org.apache.helix.metaclient.TestUtil;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.recipes.leaderelection.LeaderElectionClient;
import org.apache.helix.metaclient.recipes.leaderelection.LeaderInfo;
import org.apache.helix.zookeeper.impl.ZkTestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkLeaderElectionRecipe extends ZkMetaClientTestBase{

  private static final String PARTICIPANT_NAME1 = "participant_1";
  private static final String PARTICIPANT_NAME2 = "participant_2";
  private static final String LEADER_PATH = "/LEADER_ELECTION_GROUP_1";

  public LeaderElectionClient createLeaderElectionClient(String participantName) {
    MetaClientConfig.StoreType storeType = MetaClientConfig.StoreType.ZOOKEEPER;
    MetaClientConfig config = new MetaClientConfig.MetaClientConfigBuilder<>().setConnectionAddress(ZK_ADDR)
        .setStoreType(storeType).build();
    return new LeaderElectionClient(config, participantName);
  }

  /*
  * Test leader election when ZK session expire
  * */
  @Test
  public void testSessionExpire() throws Exception {
    String leaderPath = LEADER_PATH + "_testSessionExpire";
    LeaderInfo participantInfo = new LeaderInfo(PARTICIPANT_NAME1);
    participantInfo.setSimpleField("Key1", "value1");
    LeaderInfo participantInfo2 = new LeaderInfo(PARTICIPANT_NAME2);
    participantInfo2.setSimpleField("Key2", "value2");
    LeaderElectionClient clt1 = createLeaderElectionClient(PARTICIPANT_NAME1);
    LeaderElectionClient clt2 = createLeaderElectionClient(PARTICIPANT_NAME2) ;

    clt1.joinLeaderElectionParticipantPool(leaderPath, participantInfo);
    try {
      clt1.joinLeaderElectionParticipantPool(leaderPath, participantInfo); // no op
    } catch (ConcurrentModificationException ex) {
      // expected
      Assert.assertEquals(ex.getClass().getName(),
          "java.util.ConcurrentModificationException");
    }
    clt2.joinLeaderElectionParticipantPool(leaderPath, participantInfo2);
    // a leader should be up
    Assert.assertTrue(org.apache.helix.metaclient.TestUtil.verify(() -> {
      return (clt1.getLeader(leaderPath) != null);
    }, TestUtil.WAIT_DURATION));

    // session expire and reconnect
    ZkTestHelper.expireSession(((ZkMetaClient)clt1.getMetaClient()).getZkClient());

    Assert.assertTrue(org.apache.helix.metaclient.TestUtil.verify(() -> {
      return (clt1.getLeader(leaderPath) != null);
    }, TestUtil.WAIT_DURATION));
    Assert.assertNotNull(clt1.getLeaderEntryStat(leaderPath));
    Assert.assertNotNull(clt1.getLeader(leaderPath));
    // when session recreated, participant info node should maintain
    Assert.assertEquals(clt1.getParticipantInfo(leaderPath, PARTICIPANT_NAME1).getSimpleField("Key1"), "value1");
    Assert.assertEquals(clt2.getParticipantInfo(leaderPath, PARTICIPANT_NAME1).getSimpleField("Key1"), "value1");
    Assert.assertEquals(clt1.getParticipantInfo(leaderPath, PARTICIPANT_NAME2).getSimpleField("Key2"), "value2");
    Assert.assertEquals(clt2.getParticipantInfo(leaderPath, PARTICIPANT_NAME2).getSimpleField("Key2"), "value2");

  }
}
