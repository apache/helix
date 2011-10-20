package com.linkedin.clustermanager.participant;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZkUnitTestBase;
import com.linkedin.clustermanager.integration.TestDistCMMain;

public class TestDistControllerElection extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestDistCMMain.class);

  @Test(groups = { "unitTest" })
  public void testController() throws Exception
  {
    String className = getShortClassName();
    LOG.info("RUN " + className + " at " + new Date(System.currentTimeMillis()));

    final String clusterName = CLUSTER_PREFIX + "_" + className + "_" + "testController";
    String path = "/" + clusterName;
    if (_zkClient.exists(path))
    {
      _zkClient.deleteRecursive(path);
    }
    setupEmptyCluster(clusterName);

    final String controllerName = "controller_0";
    ClusterManager manager =
        new MockClusterManager(clusterName, controllerName, InstanceType.CONTROLLER, _zkClient);

    DistClusterControllerElection election = new DistClusterControllerElection(ZK_ADDR);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);

    path =
        "/" + clusterName + "/" + ClusterPropertyType.CONTROLLER.toString() + "/"
            + ControllerPropertyType.LEADER.toString();
    ZNRecord leaderRecord = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
    AssertJUnit.assertNotNull(election.getController());
    AssertJUnit.assertNull(election.getLeader());

    manager =
        new MockClusterManager(clusterName, "controller_1", InstanceType.CONTROLLER, _zkClient);
    election = new DistClusterControllerElection(ZK_ADDR);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);
    leaderRecord = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
    AssertJUnit.assertNull(election.getController());
    AssertJUnit.assertNull(election.getLeader());
    
    LOG.info("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));
  }

  @Test(groups = { "unitTest" })
  public void testControllerParticipant() throws Exception
  {
    String className = getShortClassName();
    LOG.info("RUN " + className + " at " + new Date(System.currentTimeMillis()));

    final String clusterName = CONTROLLER_CLUSTER_PREFIX + "_" + className 
                             + "_" + "testControllerParticipant";
    String path = "/" + clusterName;
    if (_zkClient.exists(path))
    {
      _zkClient.deleteRecursive(path);
    }
    setupEmptyCluster(clusterName);

    final String controllerName = "controller_0";
    ClusterManager manager =
        new MockClusterManager(clusterName, controllerName, InstanceType.CONTROLLER_PARTICIPANT, _zkClient);

    DistClusterControllerElection election = new DistClusterControllerElection(ZK_ADDR);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    election.onControllerChange(context);
    path =
        "/" + clusterName + "/" + ClusterPropertyType.CONTROLLER.toString() + "/"
            + ControllerPropertyType.LEADER.toString();
    ZNRecord leaderRecord = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
    AssertJUnit.assertNotNull(election.getController());
    AssertJUnit.assertNotNull(election.getLeader());
    
    manager =
        new MockClusterManager(clusterName, "controller_1", InstanceType.CONTROLLER_PARTICIPANT, _zkClient);
    election = new DistClusterControllerElection(ZK_ADDR);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    election.onControllerChange(context);
    leaderRecord = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
    AssertJUnit.assertNull(election.getController());
    AssertJUnit.assertNull(election.getLeader());
    
    LOG.info("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));
  }

  @Test(groups = { "unitTest" })
  public void testParticipant() throws Exception
  {
    String className = getShortClassName();
    LOG.info("RUN " + className + " at " + new Date(System.currentTimeMillis()));

    final String clusterName = CLUSTER_PREFIX + "_" + className + "_" + "testParticipant";
    String path = "/" + clusterName;
    if (_zkClient.exists(path))
    {
      _zkClient.deleteRecursive(path);
    }
    setupEmptyCluster(clusterName);

    final String controllerName = "participant_0";
    ClusterManager manager =
        new MockClusterManager(clusterName, controllerName, InstanceType.PARTICIPANT, _zkClient);

    DistClusterControllerElection election = new DistClusterControllerElection(ZK_ADDR);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);

    path =
        "/" + clusterName + "/" + ClusterPropertyType.CONTROLLER.toString() + "/"
            + ControllerPropertyType.LEADER.toString();
    ZNRecord leaderRecord = _zkClient.<ZNRecord> readData(path, true);
    AssertJUnit.assertNull(leaderRecord);
    AssertJUnit.assertNull(election.getController());
    AssertJUnit.assertNull(election.getLeader());
  }
  
  private void setupEmptyCluster(String clusterName)
  {
    String path = "/" + clusterName;
    _zkClient.createPersistent(path);
    _zkClient.createPersistent(path + "/" + ClusterPropertyType.STATEMODELDEFS.toString());
    _zkClient.createPersistent(path + "/" + ClusterPropertyType.INSTANCES.toString());
    _zkClient.createPersistent(path + "/" + ClusterPropertyType.CONFIGS.toString());
    _zkClient.createPersistent(path + "/" + ClusterPropertyType.IDEALSTATES.toString());
    _zkClient.createPersistent(path + "/" + ClusterPropertyType.EXTERNALVIEW.toString());
    _zkClient.createPersistent(path + "/" + ClusterPropertyType.LIVEINSTANCES.toString());
    _zkClient.createPersistent(path + "/" + ClusterPropertyType.CONTROLLER.toString());

    path = path + "/" + ClusterPropertyType.CONTROLLER.toString();
    _zkClient.createPersistent(path + "/" + ControllerPropertyType.MESSAGES.toString());
    _zkClient.createPersistent(path + "/" + ControllerPropertyType.HISTORY.toString());
    _zkClient.createPersistent(path + "/" + ControllerPropertyType.ERRORS.toString());
    _zkClient.createPersistent(path + "/" + ControllerPropertyType.STATUSUPDATES.toString());
  }

}
