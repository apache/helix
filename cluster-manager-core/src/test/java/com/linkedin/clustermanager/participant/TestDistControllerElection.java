package com.linkedin.clustermanager.participant;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
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

    path = PropertyPathConfig.getPath(PropertyType.CONTROLLER, clusterName, PropertyType.LEADER.toString());

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
    path = PropertyPathConfig.getPath(PropertyType.CONTROLLER, clusterName, PropertyType.LEADER.toString());
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

    path = PropertyPathConfig.getPath(PropertyType.CONTROLLER, clusterName, PropertyType.LEADER.toString());
    ZNRecord leaderRecord = _zkClient.<ZNRecord> readData(path, true);
    AssertJUnit.assertNull(leaderRecord);
    AssertJUnit.assertNull(election.getController());
    AssertJUnit.assertNull(election.getLeader());
  }
  
  private void setupEmptyCluster(String clusterName)
  {
    String path = "/" + clusterName;
    _zkClient.createPersistent(path);
    _zkClient.createPersistent(path + "/" + PropertyType.STATEMODELDEFS.toString());
    _zkClient.createPersistent(path + "/" + PropertyType.INSTANCES.toString());
    _zkClient.createPersistent(path + "/" + PropertyType.CONFIGS.toString());
    _zkClient.createPersistent(path + "/" + PropertyType.IDEALSTATES.toString());
    _zkClient.createPersistent(path + "/" + PropertyType.EXTERNALVIEW.toString());
    _zkClient.createPersistent(path + "/" + PropertyType.LIVEINSTANCES.toString());
    _zkClient.createPersistent(path + "/" + PropertyType.CONTROLLER.toString());

    path = path + "/" + PropertyType.CONTROLLER.toString();
    _zkClient.createPersistent(path + "/" + PropertyType.MESSAGES.toString());
    _zkClient.createPersistent(path + "/" + PropertyType.HISTORY.toString());
    _zkClient.createPersistent(path + "/" + PropertyType.ERRORS.toString());
    _zkClient.createPersistent(path + "/" + PropertyType.STATUSUPDATES.toString());
  }

}
