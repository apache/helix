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
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZkConnectionLost extends TaskTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestZkConnectionLost.class);

  private final AtomicReference<ZkServer> _zkServerRef = new AtomicReference<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants =  new MockParticipantManager[_numNodes];
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    createManagers();

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    HelixClusterVerifier clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(clusterVerifier.verify());

    _zkServerRef.set(_zkServer);
  }

  @Test
  public void testLostZkConnection() throws Exception {
    System.setProperty("helixmanager.waitForConnectedTimeout", "1000");
    System.setProperty("zk.session.timeout", "1000");
    String queueName = TestHelper.getTestMethodName();

    startParticipants();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuild = TaskTestUtil.buildRecurrentJobQueue(queueName, 0, 6000);
    createAndEnqueueJob(queueBuild, 3);

    _driver.start(queueBuild.build());

    restartZkServer();

    WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
    // ensure job 1 is started before stop it
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    _driver.pollForWorkflowState(scheduledQueue, 10000, TaskState.COMPLETED);
  }

  @Test(dependsOnMethods = { "testLostZkConnection" }, enabled = false)
  public void testLostZkConnectionNegative()
      throws Exception {
    System.setProperty("helixmanager.waitForConnectedTimeout", "10");
    System.setProperty("zk.session.timeout", "1000");
    String queueName = TestHelper.getTestMethodName();

    stopParticipants();
    startParticipants();

    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuild = TaskTestUtil.buildRecurrentJobQueue(queueName, 0, 6000);
    createAndEnqueueJob(queueBuild, 3);

    _driver.start(queueBuild.build());

    restartZkServer();

    WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
    // ensure job 1 is started before stop it
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();

    try{
      _driver.pollForWorkflowState(scheduledQueue, 10000, TaskState.COMPLETED);
      Assert.fail("Test failure!");
    } catch (HelixException ex) {
      // test succeed
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
            _zkServerRef.set(TestHelper.startZkServer(ZK_ADDR, null, false));
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
              .setJobCommandConfigMap(ImmutableMap.of(MockTask.TIMEOUT_CONFIG, "100"));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      queueBuild.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }
    Assert.assertEquals(currentJobNames.size(), jobCount);
    return currentJobNames;
  }
}


