package org.apache.helix.controller.stages;

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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.mock.MockHelixAdmin;
import org.apache.helix.mock.MockManager;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public class BaseStageTest {
  protected HelixManager manager;
  protected HelixDataAccessor accessor;
  protected ClusterEvent event;
  protected HelixAdmin admin;

  @BeforeClass()
  public void beforeClass() {
    String className = this.getClass().getName();
    System.out.println("START " + className.substring(className.lastIndexOf('.') + 1) + " at "
        + new Date(System.currentTimeMillis()));
  }

  @AfterClass()
  public void afterClass() {
    String className = this.getClass().getName();
    System.out.println("END " + className.substring(className.lastIndexOf('.') + 1) + " at "
        + new Date(System.currentTimeMillis()));
  }

  @BeforeMethod()
  public void setup() {
    String clusterName = "testCluster-" + UUID.randomUUID().toString();
    manager = new MockManager(clusterName);
    accessor = manager.getHelixDataAccessor();
    admin = new MockHelixAdmin(manager);
    event = new ClusterEvent("sampleEvent");
    admin.addCluster(clusterName);
  }

  protected List<IdealState> setupIdealState(int nodes, String[] resources, int partitions,
      int replicas, RebalanceMode rebalanceMode, String stateModelName, String rebalanceClassName,
      String rebalanceStrategyName) {
    List<IdealState> idealStates = new ArrayList<IdealState>();
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < nodes; i++) {
      instances.add("localhost_" + i);
    }

    for (int i = 0; i < resources.length; i++) {
      String resourceName = resources[i];
      ZNRecord record = new ZNRecord(resourceName);
      for (int p = 0; p < partitions; p++) {
        List<String> value = new ArrayList<String>();
        for (int r = 0; r < replicas; r++) {
          value.add("localhost_" + (p + r + 1) % nodes);
        }
        record.setListField(resourceName + "_" + p, value);
      }
      IdealState idealState = new IdealState(record);
      idealState.setStateModelDefRef(stateModelName);
      if (rebalanceClassName != null) {
        idealState.setRebalancerClassName(rebalanceClassName);
      }
      if (rebalanceStrategyName != null) {
        idealState.setRebalanceStrategy(rebalanceStrategyName);
      }
      idealState.setRebalanceMode(rebalanceMode);
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);
      idealState.setReplicas(String.valueOf(replicas));

      Builder keyBuilder = accessor.keyBuilder();

      accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
    }
    return idealStates;
  }

  protected List<IdealState> setupIdealState(int nodes, String[] resources, int partitions,
      int replicas, RebalanceMode rebalanceMode) {
    return setupIdealState(nodes, resources, partitions, replicas, rebalanceMode,
        BuiltInStateModelDefinitions.MasterSlave.name(), null, null);
  }

  protected List<IdealState> setupIdealState(int nodes, String[] resources, int partitions,
      int replicas, RebalanceMode rebalanceMode, String stateModelName) {
    return setupIdealState(nodes, resources, partitions, replicas, rebalanceMode,
        stateModelName, null, null);
  }

  protected List<IdealState> setupIdealState(int nodes, String[] resources, int partitions,
      int replicas, RebalanceMode rebalanceMode, String stateModelName, String rebalanceClassName) {
    return setupIdealState(nodes, resources, partitions, replicas, rebalanceMode,
      stateModelName, rebalanceClassName, null);
  }

  protected void setupLiveInstances(int numLiveInstances) {
    for (int i = 0; i < numLiveInstances; i++) {
      LiveInstance liveInstance = new LiveInstance("localhost_" + i);
      liveInstance.setSessionId("session_" + i);

      Builder keyBuilder = accessor.keyBuilder();
      accessor.setProperty(keyBuilder.liveInstance("localhost_" + i), liveInstance);
    }
  }

  protected void setupInstances(int numInstances) {
    // setup liveInstances
    for (int i = 0; i < numInstances; i++) {
      String instance = "localhost_" + i;
      InstanceConfig config = new InstanceConfig(instance);
      config.setHostName(instance);
      config.setPort("12134");
      admin.addInstance(manager.getClusterName(), config);
    }
  }

  protected void runStage(ClusterEvent event, Stage stage) {
    event.addAttribute("helixmanager", manager);
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try {
      stage.process(event);
    } catch (Exception e) {
      e.printStackTrace();
    }
    stage.postProcess();
  }

  protected void setupStateModel() {
    ZNRecord masterSlave = new StateModelConfigGenerator().generateConfigForMasterSlave();

    Builder keyBuilder = accessor.keyBuilder();
    accessor.setProperty(keyBuilder.stateModelDef(masterSlave.getId()),
        new StateModelDefinition(masterSlave));

    ZNRecord leaderStandby = new StateModelConfigGenerator().generateConfigForLeaderStandby();
    accessor.setProperty(keyBuilder.stateModelDef(leaderStandby.getId()),
        new StateModelDefinition(leaderStandby));

    ZNRecord onlineOffline = new StateModelConfigGenerator().generateConfigForOnlineOffline();
    accessor.setProperty(keyBuilder.stateModelDef(onlineOffline.getId()),
        new StateModelDefinition(onlineOffline));
  }

  protected Map<String, Resource> getResourceMap() {
    Map<String, Resource> resourceMap = new HashMap<String, Resource>();
    Resource testResource = new Resource("testResourceName");
    testResource.setStateModelDefRef("MasterSlave");
    testResource.addPartition("testResourceName_0");
    testResource.addPartition("testResourceName_1");
    testResource.addPartition("testResourceName_2");
    testResource.addPartition("testResourceName_3");
    testResource.addPartition("testResourceName_4");
    resourceMap.put("testResourceName", testResource);

    return resourceMap;
  }

  protected Map<String, Resource> getResourceMap(String[] resources, int partitions,
      String stateModel) {
    Map<String, Resource> resourceMap = new HashMap<String, Resource>();

    for (String r : resources) {
      Resource testResource = new Resource(r);
      testResource.setStateModelDefRef(stateModel);
      for (int i = 0; i < partitions; i++) {
        testResource.addPartition(r + "_" + i);
      }
      resourceMap.put(r, testResource);
    }

    return resourceMap;
  }
}
