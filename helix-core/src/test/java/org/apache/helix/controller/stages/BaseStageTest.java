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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.Mocks;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public class BaseStageTest {
  protected HelixManager manager;
  protected HelixDataAccessor accessor;
  protected ClusterEvent event;

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
    manager = new Mocks.MockManager(clusterName);
    accessor = manager.getHelixDataAccessor();
    event = new ClusterEvent("sampleEvent");
  }

  protected List<IdealState> setupIdealState(int nodes, String[] resources, int partitions,
      int replicas, RebalanceMode rebalanceMode) {
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
      idealState.setStateModelDefId(StateModelDefId.from("MasterSlave"));
      idealState.setRebalanceMode(rebalanceMode);
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);

      // System.out.println(idealState);

      Builder keyBuilder = accessor.keyBuilder();

      accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
    }
    return idealStates;
  }

  protected void setupLiveInstances(int numLiveInstances) {
    // setup liveInstances
    for (int i = 0; i < numLiveInstances; i++) {
      String instanceName = "localhost_" + i;
      InstanceConfig instanceConfig = new InstanceConfig(ParticipantId.from(instanceName));
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort(Integer.toString(i));
      LiveInstance liveInstance = new LiveInstance(instanceName);
      liveInstance.setSessionId("session_" + i);

      Builder keyBuilder = accessor.keyBuilder();
      accessor.setProperty(keyBuilder.instanceConfig(instanceName), instanceConfig);
      accessor.setProperty(keyBuilder.liveInstance(instanceName), liveInstance);
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

  protected Map<StateModelDefId, StateModelDefinition> setupStateModel() {
    Builder keyBuilder = accessor.keyBuilder();
    Map<StateModelDefId, StateModelDefinition> defs =
        new HashMap<StateModelDefId, StateModelDefinition>();

    ZNRecord masterSlave = StateModelConfigGenerator.generateConfigForMasterSlave();
    StateModelDefinition masterSlaveDef = new StateModelDefinition(masterSlave);
    defs.put(StateModelDefId.from(masterSlaveDef.getId()), masterSlaveDef);
    accessor.setProperty(keyBuilder.stateModelDef(masterSlave.getId()), masterSlaveDef);

    ZNRecord leaderStandby = StateModelConfigGenerator.generateConfigForLeaderStandby();
    StateModelDefinition leaderStandbyDef = new StateModelDefinition(leaderStandby);
    defs.put(StateModelDefId.from(leaderStandbyDef.getId()), leaderStandbyDef);
    accessor.setProperty(keyBuilder.stateModelDef(leaderStandby.getId()), leaderStandbyDef);

    ZNRecord onlineOffline = StateModelConfigGenerator.generateConfigForOnlineOffline();
    StateModelDefinition onlineOfflineDef = new StateModelDefinition(onlineOffline);
    defs.put(StateModelDefId.from(onlineOfflineDef.getId()), onlineOfflineDef);
    accessor.setProperty(keyBuilder.stateModelDef(onlineOffline.getId()), onlineOfflineDef);

    return defs;
  }

  protected Map<ResourceId, ResourceConfig> getResourceMap(List<IdealState> idealStates) {
    Map<ResourceId, ResourceConfig> resourceMap = new HashMap<ResourceId, ResourceConfig>();
    for (IdealState idealState : idealStates) {
      ResourceId resourceId = idealState.getResourceId();
      ResourceConfig resourceConfig =
          new ResourceConfig.Builder(resourceId).idealState(idealState)
              .userConfig(new UserConfig(Scope.resource(resourceId))).build();
      resourceMap.put(resourceId, resourceConfig);
    }

    return resourceMap;
  }
}
