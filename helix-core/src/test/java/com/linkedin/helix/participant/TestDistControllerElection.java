package com.linkedin.helix.participant;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.integration.TestDistCMMain;
import com.linkedin.helix.participant.DistClusterControllerElection;

public class TestDistControllerElection extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestDistCMMain.class);

	ZkClient _zkClient;

	@BeforeClass
	public void beforeClass()
	{
		_zkClient = new ZkClient(ZK_ADDR);
		_zkClient.setZkSerializer(new ZNRecordSerializer());
	}

	@AfterClass
	public void afterClass()
	{
		_zkClient.close();
	}

  @Test()
  public void testController() throws Exception
  {
  	System.out.println("START TestDistControllerElection at " + new Date(System.currentTimeMillis()));
    String className = getShortClassName();

    final String clusterName = CLUSTER_PREFIX + "_" + className + "_" + "testController";
    String path = "/" + clusterName;
    if (_zkClient.exists(path))
    {
      _zkClient.deleteRecursive(path);
    }
    TestHelper.setupEmptyCluster(_zkClient, clusterName);

    final String controllerName = "controller_0";
    ClusterManager manager =
        new MockZkClusterManager(clusterName, controllerName, InstanceType.CONTROLLER, _zkClient);

    DistClusterControllerElection election = new DistClusterControllerElection(ZK_ADDR);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);

    path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);

    ZNRecord leaderRecord = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
//    AssertJUnit.assertNotNull(election.getController());
//    AssertJUnit.assertNull(election.getLeader());

    manager =
        new MockZkClusterManager(clusterName, "controller_1", InstanceType.CONTROLLER, _zkClient);
    election = new DistClusterControllerElection(ZK_ADDR);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);
    leaderRecord = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
//    AssertJUnit.assertNull(election.getController());
//    AssertJUnit.assertNull(election.getLeader());

    System.out.println("END TestDistControllerElection at " + new Date(System.currentTimeMillis()));
  }

  @Test()
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
    TestHelper.setupEmptyCluster(_zkClient, clusterName);

    final String controllerName = "controller_0";
    ClusterManager manager =
        new MockZkClusterManager(clusterName, controllerName, InstanceType.CONTROLLER_PARTICIPANT, _zkClient);

    DistClusterControllerElection election = new DistClusterControllerElection(ZK_ADDR);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    election.onControllerChange(context);
    path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);
    ZNRecord leaderRecord = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
//    AssertJUnit.assertNotNull(election.getController());
//    AssertJUnit.assertNotNull(election.getLeader());

    manager =
        new MockZkClusterManager(clusterName, "controller_1", InstanceType.CONTROLLER_PARTICIPANT, _zkClient);
    election = new DistClusterControllerElection(ZK_ADDR);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    election.onControllerChange(context);
    leaderRecord = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
//    AssertJUnit.assertNull(election.getController());
//    AssertJUnit.assertNull(election.getLeader());

    LOG.info("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));
  }

  @Test()
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
    TestHelper.setupEmptyCluster(_zkClient, clusterName);

    final String controllerName = "participant_0";
    ClusterManager manager =
        new MockZkClusterManager(clusterName, controllerName, InstanceType.PARTICIPANT, _zkClient);

    DistClusterControllerElection election = new DistClusterControllerElection(ZK_ADDR);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);

    path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);
    ZNRecord leaderRecord = _zkClient.<ZNRecord> readData(path, true);
    AssertJUnit.assertNull(leaderRecord);
//    AssertJUnit.assertNull(election.getController());
//    AssertJUnit.assertNull(election.getLeader());
  }

}
