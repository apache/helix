package org.apache.helix.examples;

import java.util.List;
import java.util.Map;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Partition;
import org.apache.helix.api.Scope;
import org.apache.helix.api.State;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.accessor.ParticipantAccessor;
import org.apache.helix.api.accessor.ResourceAccessor;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.rebalancer.context.FullAutoRebalancerContext;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.Transition;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

/**
 * Example showing all major interactions with the new Helix logical model
 */
public class NewModelExample {
  private static final Logger LOG = Logger.getLogger(NewModelExample.class);

  public static void main(String[] args) {
    if (args.length < 1) {
      LOG.error("USAGE: NewModelExample zkAddress");
      System.exit(1);
    }

    // get a state model definition
    StateModelDefinition lockUnlock = getLockUnlockModel();

    // set up a resource with the state model definition
    ResourceConfig resource = getResource(lockUnlock);

    // set up a participant
    ParticipantConfig participant = getParticipant();

    // cluster id should be unique
    ClusterId clusterId = ClusterId.from("exampleCluster");

    // a user config is an object that stores arbitrary keys and values for a scope
    // in this case, the scope is the cluster with id clusterId
    // this is optional
    UserConfig userConfig = new UserConfig(Scope.cluster(clusterId));
    userConfig.setIntField("sampleInt", 1);

    // fully specify the cluster with a ClusterConfig
    ClusterConfig.Builder clusterBuilder =
        new ClusterConfig.Builder(clusterId).addResource(resource).addParticipant(participant)
            .addStateModelDefinition(lockUnlock).userConfig(userConfig);

    // add a state constraint that is more restrictive than what is in the state model
    clusterBuilder.addStateUpperBoundConstraint(Scope.cluster(clusterId),
        lockUnlock.getStateModelDefId(), State.from("LOCKED"), 1);

    // add a transition constraint (this time with a resource scope)
    clusterBuilder.addTransitionConstraint(Scope.resource(resource.getId()),
        lockUnlock.getStateModelDefId(), Transition.from("RELEASED-LOCKED"), 1);

    ClusterConfig cluster = clusterBuilder.build();

    // set up accessors to work with ZooKeeper-persisted data
    ZkClient zkClient = ZKClientPool.getZkClient(args[0]);
    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<ZNRecord>(zkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterId.stringify(), baseDataAccessor);

    // create the cluster
    createCluster(cluster, accessor);

    // update the resource
    updateResource(resource, accessor);

    // update the participant
    updateParticipant(participant, accessor);
  }

  private static void updateParticipant(ParticipantConfig participant,
      HelixDataAccessor helixAccessor) {
    // add a tag to the participant and change the hostname, then update it using a delta
    ParticipantAccessor accessor = new ParticipantAccessor(helixAccessor);
    ParticipantConfig.Delta delta =
        new ParticipantConfig.Delta(participant.getId()).addTag("newTag").setHostName("newHost");
    accessor.updateParticipant(participant.getId(), delta);
  }

  private static void updateResource(ResourceConfig resource, HelixDataAccessor helixAccessor) {
    // add some fields to the resource user config, then update it using the resource config delta
    ResourceAccessor accessor = new ResourceAccessor(helixAccessor);
    UserConfig userConfig = resource.getUserConfig();
    Map<String, String> mapField = Maps.newHashMap();
    mapField.put("k1", "v1");
    mapField.put("k2", "v2");
    userConfig.setMapField("sampleMap", mapField);
    ResourceConfig.Delta delta =
        new ResourceConfig.Delta(resource.getId()).setUserConfig(userConfig);
    accessor.updateResource(resource.getId(), delta);
  }

  private static void createCluster(ClusterConfig cluster, HelixDataAccessor helixAccessor) {
    ClusterAccessor accessor = new ClusterAccessor(cluster.getId(), helixAccessor);
    accessor.createCluster(cluster);
  }

  private static ParticipantConfig getParticipant() {
    // identify the participant
    ParticipantId participantId = ParticipantId.from("localhost_0");

    // create (optional) participant user config properties
    UserConfig userConfig = new UserConfig(Scope.participant(participantId));
    List<String> sampleList = Lists.newArrayList("elem1", "elem2");
    userConfig.setListField("sampleList", sampleList);

    // create the configuration of a new participant
    ParticipantConfig.Builder participantBuilder =
        new ParticipantConfig.Builder(participantId).hostName("localhost").port(0)
            .userConfig(userConfig);
    return participantBuilder.build();
  }

  private static ResourceConfig getResource(StateModelDefinition stateModelDef) {
    // identify the resource
    ResourceId resourceId = ResourceId.from("exampleResource");

    // create a partition
    Partition partition1 = new Partition(PartitionId.from(resourceId, "1"));

    // create a second partition
    Partition partition2 = new Partition(PartitionId.from(resourceId, "2"));

    // specify the rebalancer configuration
    // this resource will be rebalanced in FULL_AUTO mode, so use the FullAutoRebalancerContext
    // builder
    FullAutoRebalancerContext.Builder rebalanceContextBuilder =
        new FullAutoRebalancerContext.Builder(resourceId).replicaCount(1).addPartition(partition1)
            .addPartition(partition2).stateModelDefId(stateModelDef.getStateModelDefId());

    // create (optional) user-defined configuration properties for the resource
    UserConfig userConfig = new UserConfig(Scope.resource(resourceId));
    userConfig.setBooleanField("sampleBoolean", true);

    // create the configuration for a new resource
    ResourceConfig.Builder resourceBuilder =
        new ResourceConfig.Builder(resourceId).rebalancerContext(rebalanceContextBuilder.build())
            .userConfig(userConfig);
    return resourceBuilder.build();
  }

  private static StateModelDefinition getLockUnlockModel() {
    final State LOCKED = State.from("LOCKED");
    final State RELEASED = State.from("RELEASED");
    final State DROPPED = State.from("DROPPED");
    StateModelDefId stateModelId = StateModelDefId.from("LockUnlock");
    StateModelDefinition.Builder stateModelBuilder =
        new StateModelDefinition.Builder(stateModelId).addState(LOCKED, 0).addState(RELEASED, 1)
            .addState(DROPPED, 2).addTransition(RELEASED, LOCKED, 0)
            .addTransition(LOCKED, RELEASED, 1).upperBound(LOCKED, 2).upperBound(RELEASED, -1)
            .upperBound(DROPPED, -1).initialState(RELEASED);
    return stateModelBuilder.build();
  }
}
