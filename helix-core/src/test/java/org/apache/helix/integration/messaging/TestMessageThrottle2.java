package org.apache.helix.integration.messaging;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ControllerChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMessageThrottle2 extends ZkTestBase {
  private final static String _clusterName = "TestMessageThrottle2";
  private final static String _resourceName = "MyResource";
  private HelixManager _helixController;

  @Test
  public void test() throws Exception {
    System.out.println("START " + _clusterName + " at " + new Date(System.currentTimeMillis()));

    // Keep mock participant references so that they could be shut down after testing
    Set<MyProcess> participants = new HashSet<>();

    startAdmin();
    startController();

    // start node2 first
    participants.add(Node.main(new String[] {
        "2"
    }));

    // wait for node2 becoming MASTER
    final Builder keyBuilder = new Builder(_clusterName);
    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    TestHelper.verify(() -> {
      ExternalView view = accessor.getProperty(keyBuilder.externalView(_resourceName));
      String state = null;

      if (view != null) {
        Map<String, String> map = view.getStateMap(_resourceName);
        if (map != null) {
          state = map.get("node2");
        }
      }
      return state != null && state.equals("MASTER");
    }, 10 * 1000);

    // start node 1
    participants.add(Node.main(new String[] {
        "1"
    }));

    boolean result = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, _clusterName));
    Assert.assertTrue(result);

    // Clean up after testing
    _helixController.disconnect();
    participants.forEach(MyProcess::stop);
    deleteCluster(_clusterName);
    System.out.println("END " + _clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private void startController() throws Exception {
    // start _helixController
    System.out.println(String.format("Starting Controller{Cluster:%s, Port:%s, Zookeeper:%s}",
        _clusterName, 12000, ZK_ADDR));
    _helixController = HelixControllerMain.startHelixController(ZK_ADDR, _clusterName,
        "localhost_" + 12000, HelixControllerMain.STANDALONE);

    // StatusPrinter statusPrinter = new StatusPrinter();
    // statusPrinter.registerWith(_helixController);
  }

  private void startAdmin() {
    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);

    // create cluster
    System.out.println("Creating cluster: " + _clusterName);
    admin.addCluster(_clusterName, true);

    // add MasterSlave state mode definition
    admin.addStateModelDef(_clusterName, "MasterSlave",
        new StateModelDefinition(generateConfigForMasterSlave()));

    // ideal-state znrecord
    ZNRecord record = new ZNRecord(_resourceName);
    record.setSimpleField("IDEAL_STATE_MODE", "AUTO");
    record.setSimpleField("NUM_PARTITIONS", "1");
    record.setSimpleField("REPLICAS", "2");
    record.setSimpleField("STATE_MODEL_DEF_REF", "MasterSlave");
    record.setListField(_resourceName, Arrays.asList("node1", "node2"));

    admin.setResourceIdealState(_clusterName, _resourceName, new IdealState(record));

    ConstraintItemBuilder builder = new ConstraintItemBuilder();

    // limit one transition message at a time across the entire cluster
    builder.addConstraintAttribute("MESSAGE_TYPE", "STATE_TRANSITION")
        // .addConstraintAttribute("INSTANCE", ".*") // un-comment this line if using instance-level
        // constraint
        .addConstraintAttribute("CONSTRAINT_VALUE", "1");
    admin.setConstraint(_clusterName, ClusterConstraints.ConstraintType.MESSAGE_CONSTRAINT,
        "constraint1", builder.build());
  }

  private ZNRecord generateConfigForMasterSlave() {
    ZNRecord record = new ZNRecord("MasterSlave");
    record.setSimpleField(
        StateModelDefinition.StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
    List<String> statePriorityList = new ArrayList<>();
    statePriorityList.add("MASTER");
    statePriorityList.add("SLAVE");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    statePriorityList.add("ERROR");
    record.setListField(
        StateModelDefinition.StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
        statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<>();
      switch (state) {
      case "MASTER":
        metadata.put("count", "1");
        record.setMapField(key, metadata);
        break;
      case "SLAVE":
        metadata.put("count", "R");
        record.setMapField(key, metadata);
        break;
      case "OFFLINE":
      case "DROPPED":
      case "ERROR":
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
        break;
      }
    }
    for (String state : statePriorityList) {
      String key = state + ".next";
      switch (state) {
      case "MASTER": {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("SLAVE", "SLAVE");
        metadata.put("OFFLINE", "SLAVE");
        metadata.put("DROPPED", "SLAVE");
        record.setMapField(key, metadata);
        break;
      }
      case "SLAVE": {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("MASTER", "MASTER");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
        break;
      }
      case "OFFLINE": {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("SLAVE", "SLAVE");
        metadata.put("MASTER", "SLAVE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
        break;
      }
      case "ERROR": {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("OFFLINE", "OFFLINE");
        record.setMapField(key, metadata);
        break;
      }
      }
    }

    // change the transition priority list
    List<String> stateTransitionPriorityList = new ArrayList<>();
    stateTransitionPriorityList.add("SLAVE-MASTER");
    stateTransitionPriorityList.add("OFFLINE-SLAVE");
    stateTransitionPriorityList.add("MASTER-SLAVE");
    stateTransitionPriorityList.add("SLAVE-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");
    record.setListField(
        StateModelDefinition.StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);
    return record;
  }

  static final class MyProcess {
    private final String _instanceName;
    private HelixManager _helixManager;

    public MyProcess(String instanceName) {
      this._instanceName = instanceName;
    }

    public void start() throws Exception {
      _helixManager =
          new ZKHelixManager(_clusterName, _instanceName, InstanceType.PARTICIPANT, ZK_ADDR);
      {
        // hack to set sessionTimeout
        Field sessionTimeout = ZKHelixManager.class.getDeclaredField("_sessionTimeout");
        sessionTimeout.setAccessible(true);
        sessionTimeout.setInt(_helixManager, 1000);
      }

      StateMachineEngine stateMach = _helixManager.getStateMachineEngine();
      stateMach.registerStateModelFactory("MasterSlave", new MyStateModelFactory(_helixManager));
      _helixManager.connect();

      // StatusPrinter statusPrinter = new StatusPrinter();
      // statusPrinter.registerWith(_helixManager);
    }

    public void stop() {
      _helixManager.disconnect();
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "ERROR"
  })
  public static class MyStateModel extends StateModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyStateModel.class);

    private final HelixManager helixManager;

    public MyStateModel(HelixManager helixManager) {
      this.helixManager = helixManager;
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      LOGGER.info(instanceName + " becomes SLAVE from OFFLINE for " + partitionName);
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      LOGGER.info(instanceName + " becomes SLAVE from MASTER for " + partitionName);
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      LOGGER.info(instanceName + " becomes MASTER from SLAVE for " + partitionName);
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      LOGGER.info(instanceName + " becomes OFFLINE from SLAVE for " + partitionName);
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      LOGGER.info(instanceName + " becomes DROPPED from OFFLINE for " + partitionName);
    }

    @Transition(to = "OFFLINE", from = "ERROR")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      LOGGER.info(instanceName + " becomes OFFLINE from ERROR for " + partitionName);
    }
  }

  static class MyStateModelFactory extends StateModelFactory<MyStateModel> {
    private final HelixManager helixManager;

    public MyStateModelFactory(HelixManager helixManager) {
      this.helixManager = helixManager;
    }

    @Override
    public MyStateModel createNewStateModel(String resource, String partitionName) {
      return new MyStateModel(helixManager);
    }
  }

  static class Node {
    // ------------------------------ FIELDS ------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    // -------------------------- INNER CLASSES --------------------------

    // --------------------------- main() method ---------------------------

    public static MyProcess main(String[] args) throws Exception {
      if (args.length < 1) {
        LOGGER.info("usage: id");
        System.exit(0);
      }
      int id = Integer.parseInt(args[0]);
      String instanceName = "node" + id;

      addInstanceConfig(instanceName);
      // Return the thread so that it could be interrupted after testing
      return startProcess(instanceName);
    }

    private static void addInstanceConfig(String instanceName) {
      // add node to cluster if not already added
      ZKHelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);

      InstanceConfig instanceConfig = null;
      try {
        instanceConfig = admin.getInstanceConfig(_clusterName, instanceName);
      } catch (Exception ignored) {
      }
      if (instanceConfig == null) {
        InstanceConfig config = new InstanceConfig(instanceName);
        config.setHostName("localhost");
        config.setInstanceEnabled(true);
        echo("Adding InstanceConfig:" + config);
        admin.addInstance(_clusterName, config);
      }
    }

    public static void echo(Object obj) {
      LOGGER.info(obj.toString());
    }

    private static MyProcess startProcess(String instanceName) throws Exception {
      MyProcess process = new MyProcess(instanceName);
      process.start();
      return process;
    }
  }

  static class StatusPrinter implements IdealStateChangeListener, InstanceConfigChangeListener,
      ExternalViewChangeListener, LiveInstanceChangeListener, ControllerChangeListener {
    // ------------------------------ FIELDS ------------------------------

    // ------------------------ INTERFACE METHODS ------------------------

    // --------------------- Interface ControllerChangeListener
    // ---------------------

    @Override
    public void onControllerChange(NotificationContext changeContext) {
      System.out.println("StatusPrinter.onControllerChange:" + changeContext);
    }

    // --------------------- Interface ExternalViewChangeListener
    // ---------------------

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList,
        NotificationContext changeContext) {
      for (ExternalView externalView : externalViewList) {
        System.out
            .println("StatusPrinter.onExternalViewChange:" + "externalView = " + externalView);
      }
    }

    // --------------------- Interface IdealStateChangeListener
    // ---------------------

    @Override
    public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
      for (IdealState state : idealState) {
        System.out.println("StatusPrinter.onIdealStateChange:" + "state = " + state);
      }
    }

    // --------------------- Interface InstanceConfigChangeListener
    // ---------------------

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
        NotificationContext context) {
      for (InstanceConfig instanceConfig : instanceConfigs) {
        System.out.println(
            "StatusPrinter.onInstanceConfigChange:" + "instanceConfig = " + instanceConfig);
      }
    }

    // --------------------- Interface LiveInstanceChangeListener
    // ---------------------

    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances,
        NotificationContext changeContext) {
      for (LiveInstance liveInstance : liveInstances) {
        System.out
            .println("StatusPrinter.onLiveInstanceChange:" + "liveInstance = " + liveInstance);
      }
    }

    // -------------------------- OTHER METHODS --------------------------

    void registerWith(HelixManager helixManager) throws Exception {
      helixManager.addIdealStateChangeListener(this);
      helixManager.addInstanceConfigChangeListener(this);
      helixManager.addExternalViewChangeListener(this);
      helixManager.addLiveInstanceChangeListener(this);
      helixManager.addControllerListener(this);
    }
  }
}
