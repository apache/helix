package com.linkedin.helix.participant;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.model.LiveInstance;

public class TestDistControllerElection extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestDistControllerElection.class);

  @Test()
  public void testController() throws Exception
  {
    System.out.println("START TestDistControllerElection at "
        + new Date(System.currentTimeMillis()));
    String className = getShortClassName();

    final String clusterName = CLUSTER_PREFIX + "_" + className + "_" + "testController";
    String path = "/" + clusterName;
    if (_gZkClient.exists(path))
    {
      _gZkClient.deleteRecursive(path);
    }
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    final String controllerName = "controller_0";
    HelixManager manager = new MockZKHelixManager(clusterName, controllerName,
                               InstanceType.CONTROLLER,
                               _gZkClient);

    DistClusterControllerElection election = new DistClusterControllerElection(ZK_ADDR);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);

//    path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);
//    ZNRecord leaderRecord = _gZkClient.<ZNRecord> readData(path);
    LiveInstance liveInstance = accessor.getProperty(LiveInstance.class, PropertyType.LEADER);
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());
    // AssertJUnit.assertNotNull(election.getController());
    // AssertJUnit.assertNull(election.getLeader());

    manager = new MockZKHelixManager(clusterName, "controller_1", InstanceType.CONTROLLER,
                               _gZkClient);
    election = new DistClusterControllerElection(ZK_ADDR);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);
//    leaderRecord = _gZkClient.<ZNRecord> readData(path);
    liveInstance = accessor.getProperty(LiveInstance.class, PropertyType.LEADER);
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());
    // AssertJUnit.assertNull(election.getController());
    // AssertJUnit.assertNull(election.getLeader());

    System.out.println("END TestDistControllerElection at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testControllerParticipant() throws Exception
  {
    String className = getShortClassName();
    LOG.info("RUN " + className + " at " + new Date(System.currentTimeMillis()));

    final String clusterName = CONTROLLER_CLUSTER_PREFIX + "_" + className + "_"
        + "testControllerParticipant";
    String path = "/" + clusterName;
    if (_gZkClient.exists(path))
    {
      _gZkClient.deleteRecursive(path);
    }
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    final String controllerName = "controller_0";
    HelixManager manager = new MockZKHelixManager(clusterName, controllerName,
                               InstanceType.CONTROLLER_PARTICIPANT,
                               _gZkClient);

    DistClusterControllerElection election = new DistClusterControllerElection(ZK_ADDR);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    election.onControllerChange(context);

    LiveInstance liveInstance = accessor.getProperty(LiveInstance.class, PropertyType.LEADER);
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

//    path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);
//    ZNRecord leaderRecord = _gZkClient.<ZNRecord> readData(path);
//    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
    // AssertJUnit.assertNotNull(election.getController());
    // AssertJUnit.assertNotNull(election.getLeader());

    manager = new MockZKHelixManager(clusterName, "controller_1",
                               InstanceType.CONTROLLER_PARTICIPANT,
                               _gZkClient);
    election = new DistClusterControllerElection(ZK_ADDR);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    election.onControllerChange(context);

    liveInstance = accessor.getProperty(LiveInstance.class, PropertyType.LEADER);
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

//    leaderRecord = _gZkClient.<ZNRecord> readData(path);
//    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
    // AssertJUnit.assertNull(election.getController());
    // AssertJUnit.assertNull(election.getLeader());

    LOG.info("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testParticipant() throws Exception
  {
    String className = getShortClassName();
    LOG.info("RUN " + className + " at " + new Date(System.currentTimeMillis()));

    final String clusterName = CLUSTER_PREFIX + "_" + className + "_" + "testParticipant";
    String path = "/" + clusterName;
    if (_gZkClient.exists(path))
    {
      _gZkClient.deleteRecursive(path);
    }
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    final String controllerName = "participant_0";
    HelixManager manager = new MockZKHelixManager(clusterName, controllerName,
                               InstanceType.PARTICIPANT,
                               _gZkClient);

    DistClusterControllerElection election = new DistClusterControllerElection(ZK_ADDR);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);

    path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);
    ZNRecord leaderRecord = _gZkClient.<ZNRecord> readData(path, true);
    AssertJUnit.assertNull(leaderRecord);
    // AssertJUnit.assertNull(election.getController());
    // AssertJUnit.assertNull(election.getLeader());
  }

}
