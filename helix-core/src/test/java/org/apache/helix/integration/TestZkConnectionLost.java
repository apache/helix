package org.apache.helix.integration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.HelixException;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZkConnectionLost extends TaskTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestZkConnectionLost.class);

  private final AtomicReference<ZkServer> _zkServerRef = new AtomicReference<>();

  private String _zkAddr = "localhost:2189";
  ClusterSetup _setupTool;
  ZkClient _zkClient;


  @BeforeClass
  public void beforeClass() throws Exception {
    ZkServer zkServer = TestHelper.startZkServer(_zkAddr);
    _zkServerRef.set(zkServer);
    _zkClient = new ZkClient(_zkAddr);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    _setupTool = new ClusterSetup(_zkClient);
    _participants =  new MockParticipantManager[_numNodes];
    _setupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants(_setupTool);
    setupDBs(_setupTool);
    createManagers(_zkAddr, CLUSTER_NAME);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(_zkAddr, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    ZkHelixClusterVerifier clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(_zkAddr).build();
    Assert.assertTrue(clusterVerifier.verifyByPolling());
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }
    stopParticipants();

    String namespace = "/" + CLUSTER_NAME;
    if (_zkClient.exists(namespace)) {
      try {
        _setupTool.deleteCluster(CLUSTER_NAME);
      } catch (Exception ex) {
        System.err.println(
            "Failed to delete cluster " + CLUSTER_NAME + ", error: " + ex.getLocalizedMessage());
      }
    }
    _zkClient.close();
    TestHelper.stopZkServer(_zkServerRef.get());
  }

  @Test
  public void testLostZkConnection() throws Exception {
    System.setProperty(SystemPropertyKeys.ZK_WAIT_CONNECTED_TIMEOUT, "1000");
    System.setProperty(SystemPropertyKeys.ZK_SESSION_TIMEOUT, "1000");
    try {
      String queueName = TestHelper.getTestMethodName();

      startParticipants(_zkAddr);

      // Create a queue
      LOG.info("Starting job-queue: " + queueName);
      JobQueue.Builder queueBuild = TaskTestUtil.buildRecurrentJobQueue(queueName, 0, 6000);
      createAndEnqueueJob(queueBuild, 3);

      _driver.start(queueBuild.build());

      restartZkServer();

      WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
      String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
      _driver.pollForWorkflowState(scheduledQueue, 30000, TaskState.COMPLETED);
    } finally {
      System.clearProperty(SystemPropertyKeys.ZK_WAIT_CONNECTED_TIMEOUT);
      System.clearProperty(SystemPropertyKeys.ZK_SESSION_TIMEOUT);
    }
  }

  @Test(dependsOnMethods = { "testLostZkConnection" }, enabled = false)
  public void testLostZkConnectionNegative() throws Exception {
    System.setProperty(SystemPropertyKeys.ZK_WAIT_CONNECTED_TIMEOUT, "10");
    System.setProperty(SystemPropertyKeys.ZK_SESSION_TIMEOUT, "1000");

    try {
      String queueName = TestHelper.getTestMethodName();

      stopParticipants();
      startParticipants(_zkAddr);

      LOG.info("Starting job-queue: " + queueName);
      JobQueue.Builder queueBuild = TaskTestUtil.buildRecurrentJobQueue(queueName, 0, 6000);
      createAndEnqueueJob(queueBuild, 3);

      _driver.start(queueBuild.build());

      restartZkServer();

      WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
      // ensure job 1 is started before stop it
      String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();

      try {
        _driver.pollForWorkflowState(scheduledQueue, 30000, TaskState.COMPLETED);
        Assert.fail("Test failure!");
      } catch (HelixException ex) {
        // test succeed
      }
    } finally {
      System.clearProperty(SystemPropertyKeys.ZK_WAIT_CONNECTED_TIMEOUT);
      System.clearProperty(SystemPropertyKeys.ZK_SESSION_TIMEOUT);
    }
  }

  private void restartZkServer() throws ExecutionException, InterruptedException {
    // shutdown and restart zk for a couple of times
    for (int i = 0; i < 4; i++) {
      Executors.newSingleThreadExecutor().submit(new Runnable() {
        @Override public void run() {
          try {
            Thread.sleep(300);
            System.out.println(System.currentTimeMillis() + ": Shutdown ZK server.");
            TestHelper.stopZkServer(_zkServerRef.get());
            Thread.sleep(300);
            System.out.println("Restart ZK server");
            _zkServerRef.set(TestHelper.startZkServer(_zkAddr, null, false));
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          }
        }
      }).get();
    }
  }

  private List<String> createAndEnqueueJob(JobQueue.Builder queueBuild, int jobCount) {
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < jobCount; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition))
              .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "100"));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      queueBuild.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }
    Assert.assertEquals(currentJobNames.size(), jobCount);
    return currentJobNames;
  }
}


