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
import java.util.List;
import java.util.Map;

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
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
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
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

// test case from Ming Fang
public class TestMessageThrottle2 extends ZkIntegrationTestBase {
  final static String clusterName = "TestMessageThrottle2";
  final static String resourceName = "MyResource";

  @Test
  public void test() throws Exception {
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    startAdmin();
    startController();

    // start node2 first
    Node.main(new String[] {
      "2"
    });

    // wait for node2 becoming MASTER
    final Builder keyBuilder = new Builder(clusterName);
    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        ExternalView view = accessor.getProperty(keyBuilder.externalView(resourceName));
        String state = null;

        if (view != null) {
          Map<String, String> map = view.getStateMap(resourceName);
          if (map != null) {
            state = map.get("node2");
          }
        }
        return state != null && state.equals("MASTER");
      }
    }, 10 * 1000);

    // start node 1
    Node.main(new String[] {
      "1"
    });

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  void startController() throws Exception {
    // start helixController
    System.out.println(String.format("Starting Controller{Cluster:%s, Port:%s, Zookeeper:%s}",
        clusterName, 12000, ZK_ADDR));
    HelixManager helixController =
        HelixControllerMain.startHelixController(ZK_ADDR, clusterName, "localhost_" + 12000,
            HelixControllerMain.STANDALONE);

    StatusPrinter statusPrinter = new StatusPrinter();
    statusPrinter.registerWith(helixController);
  }

  void startAdmin() throws Exception {
    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);

    // create cluster
    System.out.println("Creating cluster: " + clusterName);
    admin.addCluster(clusterName, true);

    // add MasterSlave state mode definition
    admin.addStateModelDef(clusterName, "MasterSlave", new StateModelDefinition(
        generateConfigForMasterSlave()));

    // ideal-state znrecord
    ZNRecord record = new ZNRecord(resourceName);
    record.setSimpleField("IDEAL_STATE_MODE", "AUTO");
    record.setSimpleField("NUM_PARTITIONS", "1");
    record.setSimpleField("REPLICAS", "2");
    record.setSimpleField("STATE_MODEL_DEF_REF", "MasterSlave");
    record.setListField(resourceName, Arrays.asList("node1", "node2"));

    admin.setResourceIdealState(clusterName, resourceName, new IdealState(record));

    ConstraintItemBuilder builder = new ConstraintItemBuilder();

    // limit one transition message at a time across the entire cluster
    builder.addConstraintAttribute("MESSAGE_TYPE", "STATE_TRANSITION")
    // .addConstraintAttribute("INSTANCE", ".*") // un-comment this line if using instance-level
    // constraint
        .addConstraintAttribute("CONSTRAINT_VALUE", "1");
    admin.setConstraint(clusterName, ClusterConstraints.ConstraintType.MESSAGE_CONSTRAINT,
        "constraint1", builder.build());
  }

  ZNRecord generateConfigForMasterSlave() {
    ZNRecord record = new ZNRecord("MasterSlave");
    record.setSimpleField(
        StateModelDefinition.StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
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
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("MASTER")) {
        metadata.put("count", "1");
        record.setMapField(key, metadata);
      } else if (state.equals("SLAVE")) {
        metadata.put("count", "R");
        record.setMapField(key, metadata);
      } else if (state.equals("OFFLINE")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("DROPPED")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("ERROR")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }
    for (String state : statePriorityList) {
      String key = state + ".next";
      if (state.equals("MASTER")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("SLAVE", "SLAVE");
        metadata.put("OFFLINE", "SLAVE");
        metadata.put("DROPPED", "SLAVE");
        record.setMapField(key, metadata);
      } else if (state.equals("SLAVE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("MASTER", "MASTER");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      } else if (state.equals("OFFLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("SLAVE", "SLAVE");
        metadata.put("MASTER", "SLAVE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      } else if (state.equals("ERROR")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        record.setMapField(key, metadata);
      }
    }

    // change the transition priority list
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("SLAVE-MASTER");
    stateTransitionPriorityList.add("OFFLINE-SLAVE");
    stateTransitionPriorityList.add("MASTER-SLAVE");
    stateTransitionPriorityList.add("SLAVE-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");
    record.setListField(
        StateModelDefinition.StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);
    return record;
    // ZNRecordSerializer serializer = new ZNRecordSerializer();
    // System.out.println(new String(serializer.serialize(record)));
  }

  static final class MyProcess {
    private final String instanceName;
    private HelixManager helixManager;

    public MyProcess(String instanceName) {
      this.instanceName = instanceName;
    }

    public void start() throws Exception {
      helixManager =
          new ZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, ZK_ADDR);
      {
        // hack to set sessionTimeout
        Field sessionTimeout = ZKHelixManager.class.getDeclaredField("_sessionTimeout");
        sessionTimeout.setAccessible(true);
        sessionTimeout.setInt(helixManager, 1000);
      }

      StateMachineEngine stateMach = helixManager.getStateMachineEngine();
      stateMach.registerStateModelFactory("MasterSlave", new MyStateModelFactory(helixManager));
      helixManager.connect();

      StatusPrinter statusPrinter = new StatusPrinter();
      statusPrinter.registerWith(helixManager);
    }

    public void stop() {
      helixManager.disconnect();
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "ERROR"
  })
  public static class MyStateModel extends StateModel {
    private static final Logger LOGGER = Logger.getLogger(MyStateModel.class);

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

    private static final Logger LOGGER = Logger.getLogger(Node.class);

    // -------------------------- INNER CLASSES --------------------------

    // --------------------------- main() method ---------------------------

    public static void main(String[] args) throws Exception {
      if (args.length < 1) {
        LOGGER.info("usage: id");
        System.exit(0);
      }
      int id = Integer.parseInt(args[0]);
      String instanceName = "node" + id;

      addInstanceConfig(instanceName);
      startProcess(instanceName);
    }

    private static void addInstanceConfig(String instanceName) {
      // add node to cluster if not already added
      ZKHelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);

      InstanceConfig instanceConfig = null;
      try {
        instanceConfig = admin.getInstanceConfig(clusterName, instanceName);
      } catch (Exception e) {
      }
      if (instanceConfig == null) {
        InstanceConfig config = new InstanceConfig(instanceName);
        config.setHostName("localhost");
        config.setInstanceEnabled(true);
        echo("Adding InstanceConfig:" + config);
        admin.addInstance(clusterName, config);
      }
    }

    public static void echo(Object obj) {
      LOGGER.info(obj);
    }

    private static void startProcess(String instanceName) throws Exception {
      MyProcess process = new MyProcess(instanceName);
      process.start();
    }
  }

  static class StatusPrinter implements IdealStateChangeListener, InstanceConfigChangeListener,
      ExternalViewChangeListener, LiveInstanceChangeListener, ControllerChangeListener {
    // ------------------------------ FIELDS ------------------------------

    private HelixManager helixManager;

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
        System.out.println("StatusPrinter.onInstanceConfigChange:" + "instanceConfig = "
            + instanceConfig);
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

    public void registerWith(HelixManager helixManager) throws Exception {
      this.helixManager = helixManager;
      helixManager.addIdealStateChangeListener(this);
      helixManager.addInstanceConfigChangeListener(this);
      helixManager.addExternalViewChangeListener(this);
      helixManager.addLiveInstanceChangeListener(this);
      helixManager.addControllerListener(this);
    }
  }
}
