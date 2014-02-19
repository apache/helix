package org.apache.helix.monitoring;

import java.io.IOException;
import java.util.Date;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.monitoring.mbeans.ClusterMBeanObserver;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterStatusMonitorLifecycle extends ZkIntegrationTestBase {

  MockParticipantManager[] _participants;
  ClusterDistributedController[] _controllers;
  String _controllerClusterName;
  String _clusterNamePrefix;
  String _firstClusterName;

  final int n = 5;
  final int clusterNb = 10;

  @BeforeClass
  public void beforeClass() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    _clusterNamePrefix = className + "_" + methodName;

    System.out.println("START " + _clusterNamePrefix + " at "
        + new Date(System.currentTimeMillis()));

    // setup 10 clusters
    for (int i = 0; i < clusterNb; i++) {
      String clusterName = _clusterNamePrefix + "0_" + i;
      String participantName = "localhost" + i;
      String resourceName = "TestDB" + i;
      TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
          participantName, // participant name prefix
          resourceName, // resource name prefix
          1, // resources
          8, // partitions per resource
          n, // number of nodes
          3, // replicas
          "MasterSlave", true); // do rebalance
    }

    // setup controller cluster
    _controllerClusterName = "CONTROLLER_" + _clusterNamePrefix;
    TestHelper.setupCluster("CONTROLLER_" + _clusterNamePrefix, ZK_ADDR, 0, // controller
                                                                            // port
        "controller", // participant name prefix
        _clusterNamePrefix, // resource name prefix
        1, // resources
        clusterNb, // partitions per resource
        n, // number of nodes
        3, // replicas
        "LeaderStandby", true); // do rebalance

    // start distributed cluster controllers
    _controllers = new ClusterDistributedController[n + n];
    for (int i = 0; i < n; i++) {
      _controllers[i] =
          new ClusterDistributedController(ZK_ADDR, _controllerClusterName, "controller_" + i);
      _controllers[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(
            new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, _controllerClusterName),
            30000);
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // start first cluster
    _participants = new MockParticipantManager[n];
    _firstClusterName = _clusterNamePrefix + "0_0";
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost0_" + (12918 + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _firstClusterName, instanceName);
      _participants[i].syncStart();
    }

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            _firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");

    // add more controllers to controller cluster
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    for (int i = 0; i < n; i++) {
      String controller = "controller_" + (n + i);
      setupTool.addInstanceToCluster(_controllerClusterName, controller);
    }
    setupTool.rebalanceStorageCluster(_controllerClusterName, _clusterNamePrefix + "0", 6);
    for (int i = n; i < 2 * n; i++) {
      _controllers[i] =
          new ClusterDistributedController(ZK_ADDR, _controllerClusterName, "controller_" + i);
      _controllers[i].syncStart();
    }

    // verify controller cluster
    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                _controllerClusterName));
    Assert.assertTrue(result, "Controller cluster NOT in ideal state");

    // verify first cluster
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            _firstClusterName));
    Assert.assertTrue(result, "first cluster NOT in ideal state");
  }

  @AfterClass
  public void afterClass() {
    System.out.println("Cleaning up...");
    for (int i = 0; i < 5; i++) {
      boolean result =
          ClusterStateVerifier
              .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                  _controllerClusterName));
      _controllers[i].syncStop();
    }

    for (int i = 0; i < 5; i++) {
      _participants[i].syncStop();
    }

    System.out.println("END " + _clusterNamePrefix + " at " + new Date(System.currentTimeMillis()));

  }

  class ParticipantMonitorListener extends ClusterMBeanObserver {

    int _nMbeansUnregistered = 0;
    int _nMbeansRegistered = 0;

    public ParticipantMonitorListener(String domain) throws InstanceNotFoundException, IOException,
        MalformedObjectNameException, NullPointerException {
      super(domain);
    }

    @Override
    public void onMBeanRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {
      _nMbeansRegistered++;
    }

    @Override
    public void onMBeanUnRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {
      _nMbeansUnregistered++;
    }
  }

  @Test
  public void testClusterStatusMonitorLifecycle() throws InstanceNotFoundException,
      MalformedObjectNameException, NullPointerException, IOException, InterruptedException {
    ParticipantMonitorListener listener = new ParticipantMonitorListener("ClusterStatus");

    int nMbeansUnregistered = listener._nMbeansUnregistered;
    int nMbeansRegistered = listener._nMbeansRegistered;

    _participants[0].disconnect();

    // participant goes away. should be no change in number of beans as config is still present
    Thread.sleep(1000);
    Assert.assertTrue(nMbeansUnregistered == listener._nMbeansUnregistered);
    Assert.assertTrue(nMbeansRegistered == listener._nMbeansRegistered);

    HelixDataAccessor accessor = _participants[n - 1].getHelixDataAccessor();
    String firstControllerName =
        accessor.getProperty(accessor.keyBuilder().controllerLeader()).getId();

    ClusterDistributedController firstController = null;
    for (ClusterDistributedController controller : _controllers) {
      if (controller.getInstanceName().equals(firstControllerName)) {
        firstController = controller;
      }
    }
    firstController.disconnect();
    Thread.sleep(1000);

    // 1 cluster status monitor, 1 resource monitor, 5 instances
    Assert.assertTrue(nMbeansUnregistered == listener._nMbeansUnregistered - 7);
    Assert.assertTrue(nMbeansRegistered == listener._nMbeansRegistered - 7);

    String instanceName = "localhost0_" + (12918 + 0);
    _participants[0] = new MockParticipantManager(ZK_ADDR, _firstClusterName, instanceName);
    _participants[0].syncStart();

    // participant goes back. should be no change
    Thread.sleep(1000);
    Assert.assertTrue(nMbeansUnregistered == listener._nMbeansUnregistered - 7);
    Assert.assertTrue(nMbeansRegistered == listener._nMbeansRegistered - 7);

    // Add a resource, one more mbean registered
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    IdealState idealState = accessor.getProperty(accessor.keyBuilder().idealStates("TestDB00"));

    setupTool.addResourceToCluster(_firstClusterName, "TestDB1", idealState.getNumPartitions(),
        "MasterSlave");
    setupTool.rebalanceResource(_firstClusterName, "TestDB1",
        Integer.parseInt(idealState.getReplicas()));

    Thread.sleep(1000);
    Assert.assertTrue(nMbeansUnregistered == listener._nMbeansUnregistered - 7);
    Assert.assertTrue(nMbeansRegistered == listener._nMbeansRegistered - 8);

    // remove resource, no change
    setupTool.dropResourceFromCluster(_firstClusterName, "TestDB1");
    Thread.sleep(1000);
    Assert.assertTrue(nMbeansUnregistered == listener._nMbeansUnregistered - 7);
    Assert.assertTrue(nMbeansRegistered == listener._nMbeansRegistered - 8);

  }
}
