package org.apache.helix.integration.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TaskTestBase extends ZkIntegrationTestBase {
  protected int _numNodes = 5;
  protected int _startPort = 12918;
  protected int _numParitions = 20;
  protected int _numReplicas = 3;
  protected int _numDbs = 1;

  protected ClusterControllerManager _controller;

  protected HelixManager _manager;
  protected TaskDriver _driver;
  protected ClusterSetup _setupTool;

  protected List<String> _testDbs = new ArrayList<String>();

  protected final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  protected final MockParticipantManager[] _participants = new MockParticipantManager[_numNodes];

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // Set up target db
    if (_numDbs > 1) {
      for (int i = 0; i < _numDbs; i++) {
        String db = WorkflowGenerator.DEFAULT_TGT_DB + i;
        _setupTool
            .addResourceToCluster(CLUSTER_NAME, db, _numParitions + 10 * i, MASTER_SLAVE_STATE_MODEL,
                IdealState.RebalanceMode.FULL_AUTO.toString());
        _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _numReplicas);
        _testDbs.add(db);
      }
    } else {
      _setupTool.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, _numParitions, MASTER_SLAVE_STATE_MODEL);
      _setupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, _numReplicas);
    }

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, new TaskFactory() {
      @Override public Task createNewTask(TaskCallbackContext context) {
        return new MockTask(context);
      }
    });

    // start dummy participants
    for (int i = 0; i < _numNodes; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // create cluster manager
    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _manager.disconnect();

    for (int i = 0; i < _numNodes; i++) {
      _participants[i].syncStop();
    }

    _controller.syncStop();
  }
}
