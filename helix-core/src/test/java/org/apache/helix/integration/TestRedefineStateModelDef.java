package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * test case for redefining state model definition
 * the new state model definition should be compatible with the old state model definition (i.e.
 * states and transitions of old state model definition should be a subset of the new state model
 * definition)
 */
public class TestRedefineStateModelDef extends ZkUnitTestBase {

  @Test
  public void test() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        8, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", false);
    autoRebalance(clusterName);

    // start controller
    ClusterController controller = new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);

    // stop controller, redefine state model definition, and re-start controller
    controller.syncStop();
    redefineStateModelDef(clusterName);
    controller = new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);

    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  // auto-rebalance
  private void autoRebalance(String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));

    idealState.setReplicas("" + 2);
    idealState.setRebalanceMode(RebalanceMode.FULL_AUTO);
    for (int i = 0; i < idealState.getNumPartitions(); i++) {
      String partitionName = "TestDB0_" + i;
      idealState.getRecord().setMapField(partitionName, new HashMap<String, String>());
      idealState.getRecord().setListField(partitionName, new ArrayList<String>());
    }

    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);
  }

  // redefine a new master-slave state machine
  // the new state machine adds a new LEADER state which transfers to/from MASTER
  private void redefineStateModelDef(String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    StateModelDefinition masterSlave =
        accessor.getProperty(keyBuilder.stateModelDef("MasterSlave"));
    masterSlave.getRecord().getListField("STATE_PRIORITY_LIST").add(0, "LEADER");
    masterSlave.getRecord().getListField("STATE_TRANSITION_PRIORITYLIST").add(0, "LEADER-MASTER");
    masterSlave.getRecord().getListField("STATE_TRANSITION_PRIORITYLIST").add(0, "MASTER-LEADER");
    masterSlave.getRecord().getMapFields().put("LEADER.meta", new TreeMap<String, String>());
    masterSlave.getRecord().getMapField("LEADER.meta").put("count", "1");
    masterSlave.getRecord().getMapFields().put("LEADER.next", new TreeMap<String, String>());
    masterSlave.getRecord().getMapField("LEADER.next").put("MASTER", "MASTER");
    masterSlave.getRecord().getMapField("LEADER.next").put("SLAVE", "MASTER");
    masterSlave.getRecord().getMapField("LEADER.next").put("OFFLINE", "MASTER");
    masterSlave.getRecord().getMapField("LEADER.next").put("OFFLINE", "MASTER");
    masterSlave.getRecord().getMapField("LEADER.next").put("DROPPED", "MASTER");

    masterSlave.getRecord().getMapField("MASTER.meta").put("count", "R");
    masterSlave.getRecord().getMapField("MASTER.next").put("LEADER", "LEADER");
    masterSlave.getRecord().getMapField("SLAVE.next").put("LEADER", "MASTER");
    masterSlave.getRecord().getMapField("OFFLINE.next").put("LEADER", "MASTER");

    StateModelDefinition newMasterSlave = new StateModelDefinition(masterSlave.getRecord());
    accessor.setProperty(keyBuilder.stateModelDef("MasterSlave"), newMasterSlave);
  }

}
