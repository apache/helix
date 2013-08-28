package org.apache.helix.api;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;

/**
 * Read a cluster from zookeeper
 */
public class ClusterReader {
  final ZkClient _client;

  public ClusterReader(ZkClient client) {
    _client = client;
  }

  // TODO move to ClusterAccessor
  /**
   * Read the following znodes from zookeeper and construct a cluster instance
   * - all instance-configs
   * - all ideal-states
   * - all live-instances
   * - all messages
   * - all current-states
   * @param clusterId
   * @return cluster or null if not exist
   */
  public Cluster readCluster(String clusterId) {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterId, new ZkBaseDataAccessor<ZNRecord>(_client));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    /**
     * map of instance-id to instance-config
     */
    Map<String, InstanceConfig> instanceConfigMap =
        accessor.getChildValuesMap(keyBuilder.instanceConfigs());

    /**
     * map of resource-id to ideal-state
     */
    Map<String, IdealState> idealStateMap = accessor.getChildValuesMap(keyBuilder.idealStates());

    /**
     * map of instance-id to live-instance
     */
    Map<String, LiveInstance> liveInstanceMap =
        accessor.getChildValuesMap(keyBuilder.liveInstances());

    /**
     * map of participant-id to map of message-id to message
     */
    Map<String, Map<String, Message>> messageMap = new HashMap<String, Map<String, Message>>();
    for (String instanceName : liveInstanceMap.keySet()) {
      Map<String, Message> instanceMsgMap =
          accessor.getChildValuesMap(keyBuilder.messages(instanceName));
      messageMap.put(instanceName, instanceMsgMap);
    }

    /**
     * map of resource-id to map of participant-id to current-state
     */
    Map<String, Map<String, CurrentState>> currentStateMap =
        new HashMap<String, Map<String, CurrentState>>();
    for (String participantId : liveInstanceMap.keySet()) {
      LiveInstance liveInstance = liveInstanceMap.get(participantId);
      String sessionId = liveInstance.getSessionId();
      Map<String, CurrentState> instanceCurStateMap =
          accessor.getChildValuesMap(keyBuilder.currentStates(participantId, sessionId));

      for (String resourceId : instanceCurStateMap.keySet()) {
        if (!currentStateMap.containsKey(resourceId)) {
          currentStateMap.put(resourceId, new HashMap<String, CurrentState>());
        }

        currentStateMap.get(resourceId).put(participantId, instanceCurStateMap.get(resourceId));
      }
    }

    return new Cluster(new ClusterId(clusterId), idealStateMap, currentStateMap, instanceConfigMap,
        liveInstanceMap, messageMap, null);
  }

  /**
   * simple test
   * @param args
   */
  public static void main(String[] args) {
    ZkClient client =
        new ZkClient("zzhang-ld", ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());

    ClusterReader reader = new ClusterReader(client);
    Cluster cluster = reader.readCluster("ESPRESSO_STORAGE");

    Map<ParticipantId, Participant> participantMap = cluster.getParticipantMap();
    for (ParticipantId participantId : participantMap.keySet()) {
      Participant participant = participantMap.get(participantId);
      System.out.println(participantId + " - " + participant.isEnabled());
      if (participant.isAlive()) {
        System.out.println("\t" + participant.getRunningInstance().getSessionId());
      }
    }

    Map<ResourceId, Resource> resourceMap = cluster.getResourceMap();
    for (ResourceId resourceId : resourceMap.keySet()) {
      Resource resource = resourceMap.get(resourceId);
      // System.out.println(resourceId + " - " + resource.getStateModelDefId());

      // TODO fix it
      //
      // Map<ParticipantId, CurState> curStateMap = resource.getCurrentStateMap();
      // for (ParticipantId participantId : curStateMap.keySet()) {
      // System.out.println("\t" + participantId);
      // CurState curState = curStateMap.get(participantId);
      // for (PartitionId partitionId : curState.getPartitionIdSet()) {
      // State state = curState.getState(partitionId);
      // System.out.println("\t\t" + partitionId + " - " + state);
      // }
      // }
    }
  }
}
