package org.apache.helix.examples;

import java.util.concurrent.TimeUnit;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.ClusterAccessor;
import org.apache.helix.api.ClusterConfig;
import org.apache.helix.api.ClusterId;
import org.apache.helix.api.FullAutoRebalancerConfig;
import org.apache.helix.api.Id;
import org.apache.helix.api.ParticipantConfig;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.ResourceConfig;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.State;
import org.apache.helix.api.StateModelDefId;
import org.apache.helix.api.StateModelDefinitionAccessor;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

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

public class NewModelExample {
  private static final Logger LOG = Logger.getLogger(NewModelExample.class);

  public static void main(String[] args) {
    if (args.length < 1) {
      LOG.error("USAGE: NewModelExample zkAddress");
      System.exit(1);
    }
    StateModelDefinition lockUnlock = getLockUnlockModel();
    ResourceConfig resource = getResource(lockUnlock);
    ParticipantConfig participant = getParticipant();
    ClusterId clusterId = Id.cluster("exampleCluster");
    ClusterConfig cluster =
        new ClusterConfig.Builder(clusterId).addResource(resource).addParticipant(participant)
            .build();
    int timeOutInSec = Integer.parseInt(System.getProperty(ZKHelixAdmin.CONNECTION_TIMEOUT, "30"));
    ZkClient zkClient = new ZkClient(args[0], timeOutInSec * 1000);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.waitUntilConnected(timeOutInSec, TimeUnit.SECONDS);
    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<ZNRecord>(zkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterId.stringify(), baseDataAccessor);
    persistStateModel(lockUnlock, accessor);
    createCluster(cluster, accessor);
  }

  private static void persistStateModel(StateModelDefinition stateModelDef,
      HelixDataAccessor helixAccessor) {
    StateModelDefinitionAccessor accessor = new StateModelDefinitionAccessor(helixAccessor);
    accessor.addStateModelDefinition(stateModelDef);
  }

  private static void createCluster(ClusterConfig cluster, HelixDataAccessor helixAccessor) {
    ClusterAccessor accessor = new ClusterAccessor(cluster.getId(), helixAccessor);
    accessor.createCluster(cluster);
  }

  private static ParticipantConfig getParticipant() {
    ParticipantId participantId = Id.participant("localhost_0");
    ParticipantConfig.Builder participantBuilder =
        new ParticipantConfig.Builder(participantId).hostName("localhost").port(0);
    return participantBuilder.build();
  }

  private static ResourceConfig getResource(StateModelDefinition stateModelDef) {
    ResourceId resourceId = Id.resource("exampleResource");
    FullAutoRebalancerConfig.Builder rebalanceConfigBuilder =
        new FullAutoRebalancerConfig.Builder(resourceId).replicaCount(3).addPartitions(5)
            .stateModelDef(stateModelDef.getStateModelDefId());
    ResourceConfig.Builder resourceBuilder =
        new ResourceConfig.Builder(resourceId).rebalancerConfig(rebalanceConfigBuilder.build());
    return resourceBuilder.build();
  }

  private static StateModelDefinition getLockUnlockModel() {
    final State LOCKED = State.from("LOCKED");
    final State RELEASED = State.from("RELEASED");
    final State DROPPED = State.from("DROPPED");
    StateModelDefId stateModelId = Id.stateModelDef("LockUnlock");
    StateModelDefinition.Builder stateModelBuilder =
        new StateModelDefinition.Builder(stateModelId).addState(LOCKED, 0).addState(RELEASED, 1)
            .addState(DROPPED, 2).addTransition(RELEASED, LOCKED, 0)
            .addTransition(LOCKED, RELEASED, 1).upperBound(LOCKED, 1).upperBound(RELEASED, -1)
            .upperBound(DROPPED, -1).initialState(RELEASED);
    return stateModelBuilder.build();
  }
}
