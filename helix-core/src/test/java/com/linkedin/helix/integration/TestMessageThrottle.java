package com.linkedin.helix.integration;

import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.I0Itec.zkclient.IZkChildListener;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.model.ClusterConstraints;
import com.linkedin.helix.model.ClusterConstraints.ConstraintType;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;

public class TestMessageThrottle extends ZkIntegrationTestBase
{
  @Test()
  public void testMessageThrottle() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);

    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];
    // ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start
                                                         // port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // setup message constraint
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=.*,CONSTRAINT_VALUE=1";
    ZNRecord record = new ZNRecord(ConstraintType.MESSAGE_CONSTRAINT.toString());
    record.setMapField("constraint1", new TreeMap<String, String>());
    record.getMapField("constraint1").put("MESSAGE_TYPE", "STATE_TRANSITION");
    // record.getMapField("constraint1").put("TRANSITION", "OFFLINE-SLAVE");
    record.getMapField("constraint1").put("INSTANCE", ".*");
    record.getMapField("constraint1").put("CONSTRAINT_VALUE", "1");
    ClusterConstraints constraint = new ClusterConstraints(record);

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    accessor.setProperty(keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()),
                         constraint);

    // make sure we never see more than 1 state transition message for each participant
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);
      String msgPath =
          PropertyPathConfig.getPath(PropertyType.MESSAGES, clusterName, instanceName);
      _gZkClient.subscribeChildChanges(msgPath, new IZkChildListener()
      {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
        {
          // TODO Auto-generated method stub
          Assert.assertTrue(currentChilds.size() <= 1,
                            "Should not see more than 1 message");
        }
      });
    }

    TestHelper.startController(clusterName,
                               "controller_0",
                               ZK_ADDR,
                               HelixControllerMain.STANDALONE);
    // start participants
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              clusterName));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // clean up
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
