package org.apache.helix.testutil;

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
import java.util.List;
import java.util.Map;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

public class HelixTestUtil {
  private static Logger LOG = Logger.getLogger(HelixTestUtil.class);

  /**
   * Ensures that external view and current state are empty
   */
  public static class EmptyZkVerifier extends ZkVerifier {
    private final String _resourceName;

    /**
     * Instantiate the verifier
     * @param clusterName the cluster to verify
     * @param resourceName the resource to verify
     */
    public EmptyZkVerifier(String clusterName, String resourceName, ZkClient zkclient) {
      super(clusterName, zkclient);
      _resourceName = resourceName;
    }

    @Override
    public boolean verify() {
      BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(getZkClient());
      HelixDataAccessor accessor = new ZKHelixDataAccessor(getClusterName(), baseAccessor);
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      ExternalView externalView = accessor.getProperty(keyBuilder.externalView(_resourceName));

      // verify external view empty
      if (externalView != null) {
        for (String partition : externalView.getPartitionSet()) {
          Map<String, String> stateMap = externalView.getStateMap(partition);
          if (stateMap != null && !stateMap.isEmpty()) {
            LOG.error("External view not empty for " + partition);
            return false;
          }
        }
      }

      // verify current state empty
      List<String> liveParticipants = accessor.getChildNames(keyBuilder.liveInstances());
      for (String participant : liveParticipants) {
        List<String> sessionIds = accessor.getChildNames(keyBuilder.sessions(participant));
        for (String sessionId : sessionIds) {
          CurrentState currentState =
              accessor.getProperty(keyBuilder.currentState(participant, sessionId, _resourceName));
          Map<String, String> partitionStateMap = currentState.getPartitionStateMap();
          if (partitionStateMap != null && !partitionStateMap.isEmpty()) {
            LOG.error("Current state not empty for " + participant);
            return false;
          }
        }
      }
      return true;
    }
  }

  /**
   * Poll for the existence (or lack thereof) of a specific Helix property
   * @param clazz the HelixProeprty subclass
   * @param accessor connected HelixDataAccessor
   * @param key the property key to look up
   * @param shouldExist true if the property should exist, false otherwise
   * @return the property if found, or null if it does not exist
   */
  public static <T extends HelixProperty> T pollForProperty(Class<T> clazz,
      HelixDataAccessor accessor, PropertyKey key, boolean shouldExist) throws InterruptedException {
    final int POLL_TIMEOUT = 5000;
    final int POLL_INTERVAL = 50;
    T property = accessor.getProperty(key);
    int timeWaited = 0;
    while (((shouldExist && property == null) || (!shouldExist && property != null))
        && timeWaited < POLL_TIMEOUT) {
      Thread.sleep(POLL_INTERVAL);
      timeWaited += POLL_INTERVAL;
      property = accessor.getProperty(key);
    }
    return property;
  }

  public static void setupStateModel(BaseDataAccessor<ZNRecord> baseAccessor, String clusterName) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    StateModelDefinition masterSlave =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    accessor.setProperty(keyBuilder.stateModelDef(masterSlave.getId()), masterSlave);

    StateModelDefinition leaderStandby =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForLeaderStandby());
    accessor.setProperty(keyBuilder.stateModelDef(leaderStandby.getId()), leaderStandby);

    StateModelDefinition onlineOffline =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline());
    accessor.setProperty(keyBuilder.stateModelDef(onlineOffline.getId()), onlineOffline);
  }

  public static List<IdealState> setupIdealState(BaseDataAccessor<ZNRecord> baseAccessor,
      String clusterName, int[] nodes, String[] resources, int partitions, int replicas) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    List<IdealState> idealStates = new ArrayList<IdealState>();
    List<String> instances = new ArrayList<String>();
    for (int i : nodes) {
      instances.add("localhost_" + i);
    }

    for (String resourceName : resources) {
      IdealState idealState = new IdealState(resourceName);
      for (int p = 0; p < partitions; p++) {
        List<String> value = new ArrayList<String>();
        for (int r = 0; r < replicas; r++) {
          int n = nodes[(p + r) % nodes.length];
          value.add("localhost_" + n);
        }
        idealState.getRecord().setListField(resourceName + "_" + p, value);
      }

      idealState.setReplicas(Integer.toString(replicas));
      idealState.setStateModelDefId(StateModelDefId.from("MasterSlave"));
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);

      // System.out.println(idealState);
      accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
    }
    return idealStates;
  }

  public static void setupLiveInstances(BaseDataAccessor<ZNRecord> baseAccessor,
      String clusterName, int[] liveInstances) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    for (int i = 0; i < liveInstances.length; i++) {
      String instance = "localhost_" + liveInstances[i];
      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstance.setSessionId("session_" + liveInstances[i]);
      liveInstance.setHelixVersion("0.4.0");
      accessor.setProperty(keyBuilder.liveInstance(instance), liveInstance);
    }
  }

  public static void setupInstances(BaseDataAccessor<ZNRecord> baseAccessor, String clusterName,
      int[] instances) {
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    for (int i = 0; i < instances.length; i++) {
      String instance = "localhost_" + instances[i];
      InstanceConfig instanceConfig = new InstanceConfig(instance);
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort("" + instances[i]);
      instanceConfig.setInstanceEnabled(true);
      accessor.setProperty(keyBuilder.instanceConfig(instance), instanceConfig);
    }
  }

  public static void runPipeline(ClusterEvent event, Pipeline pipeline) {
    try {
      pipeline.handle(event);
      pipeline.finish();
    } catch (Exception e) {
      LOG.error("Exception while executing pipeline:" + pipeline
          + ". Will not continue to next pipeline", e);
    }
  }


  public static void runStage(ClusterEvent event, Stage stage) throws Exception {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    stage.process(event);
    stage.postProcess();
  }

  public static Message newMessage(MessageType type, MessageId msgId, String fromState,
      String toState, String resourceName, String tgtName) {
    Message msg = new Message(type.toString(), msgId);
    msg.setFromState(State.from(fromState));
    msg.setToState(State.from(toState));
    msg.getRecord().setSimpleField(Attributes.RESOURCE_NAME.toString(), resourceName);
    msg.setTgtName(tgtName);
    return msg;
  }

  public static Message newMessage(MessageType type, MessageId msgId, String fromState,
      String toState, String resourceName, String tgtName, String partitionId) {
    Message msg = newMessage(type, msgId, fromState, toState, resourceName, tgtName);
    msg.setPartitionId(PartitionId.from(partitionId));
    return msg;
  }
}
