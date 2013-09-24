package org.apache.helix.examples;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.ClusterAccessor;
import org.apache.helix.api.ClusterConfig;
import org.apache.helix.api.ClusterId;
import org.apache.helix.api.ParticipantConfig;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.Partition;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.ResourceConfig;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.Scope;
import org.apache.helix.api.State;
import org.apache.helix.api.StateModelDefId;
import org.apache.helix.api.UserConfig;
import org.apache.helix.controller.rebalancer.context.FullAutoRebalancerContext;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.Transition;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

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
    int timeOutInSec = Integer.parseInt(System.getProperty(ZKHelixAdmin.CONNECTION_TIMEOUT, "30"));
    ZkClient zkClient = new ZkClient(args[0], timeOutInSec * 1000);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.waitUntilConnected(timeOutInSec, TimeUnit.SECONDS);
    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<ZNRecord>(zkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterId.stringify(), baseDataAccessor);

    // create the cluster
    createCluster(cluster, accessor);
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
    Partition partition1 = new Partition(PartitionId.from("partition1"));

    // create a second partition
    Partition partition2 = new Partition(PartitionId.from("partition2"));

    // specify the rebalancer configuration
    // this resource will be rebalanced in FULL_AUTO mode, so use the FullAutoRebalancerConfig
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
