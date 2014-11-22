package org.apache.helix.examples;

import java.util.List;
import java.util.Map;

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixController;
import org.apache.helix.HelixParticipant;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Scope;
import org.apache.helix.api.State;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.Transition;
import org.apache.helix.model.builder.AutoRebalanceModeISBuilder;
import org.apache.helix.participant.statemachine.StateModelInfo;
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
public class LogicalModelExample {
  private static final Logger LOG = Logger.getLogger(LogicalModelExample.class);

  public static void main(String[] args) throws InterruptedException {
    if (args.length < 1) {
      LOG.error("USAGE: LogicalModelExample zkAddress");
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
            .addStateModelDefinition(lockUnlock).userConfig(userConfig).autoJoin(true);

    // add a transition constraint (with a resource scope)
    clusterBuilder.addTransitionConstraint(Scope.resource(resource.getId()), lockUnlock.getStateModelDefId(),
        Transition.from(State.from("RELEASED"), State.from("LOCKED")), 1);

    ClusterConfig cluster = clusterBuilder.build();

    // set up a connection to work with ZooKeeper-persisted data
    HelixConnection connection = new ZkHelixConnection(args[0]);
    connection.connect();

    // create the cluster
    createCluster(cluster, connection);

    // update the resource
    updateResource(resource, clusterId, connection);

    // update the participant
    updateParticipant(participant, clusterId, connection);

    // start the controller
    ControllerId controllerId = ControllerId.from("exampleController");
    HelixController helixController = connection.createController(clusterId, controllerId);
    helixController.start();

    // start the specified participant
    HelixParticipant helixParticipant = connection.createParticipant(clusterId, participant.getId());
    helixParticipant.getStateMachineEngine().registerStateModelFactory(lockUnlock.getStateModelDefId(),
        new LockUnlockFactory());
    helixParticipant.start();

    // start another participant via auto join
    HelixParticipant autoJoinParticipant =
        connection.createParticipant(clusterId, ParticipantId.from("localhost_12120"));
    autoJoinParticipant.getStateMachineEngine().registerStateModelFactory(lockUnlock.getStateModelDefId(),
        new LockUnlockFactory());
    autoJoinParticipant.start();

    Thread.sleep(5000);
    printExternalView(connection, clusterId, resource.getId());

    // stop the participants
    helixParticipant.stop();
    autoJoinParticipant.stop();

    // stop the controller
    helixController.stop();

    // drop the cluster
    dropCluster(clusterId, connection);
    connection.disconnect();
  }

  private static void dropCluster(ClusterId clusterId, HelixConnection connection) {
    ClusterAccessor accessor = connection.createClusterAccessor(clusterId);
    accessor.dropCluster();
  }

  private static void printExternalView(HelixConnection connection, ClusterId clusterId, ResourceId resourceId) {
    ClusterAccessor accessor = connection.createClusterAccessor(clusterId);
    Cluster cluster = accessor.readCluster();
    ExternalView externalView = cluster.getResource(resourceId).getExternalView();
    System.out.println("ASSIGNMENTS:");
    for (PartitionId partitionId : externalView.getPartitionIdSet()) {
      System.out.println(partitionId + ": " + externalView.getStateMap(partitionId));
    }
  }

  private static void updateParticipant(ParticipantConfig participant, ClusterId clusterId, HelixConnection connection) {
    // add a tag to the participant and change the hostname, then update it using a delta
    ClusterAccessor accessor = connection.createClusterAccessor(clusterId);
    ParticipantConfig.Delta delta =
        new ParticipantConfig.Delta(participant.getId()).addTag("newTag").setHostName("newHost");
    accessor.updateParticipant(participant.getId(), delta);
  }

  private static void updateResource(ResourceConfig resource, ClusterId clusterId, HelixConnection connection) {
    // add some fields to the resource user config, then update it using the resource config delta
    ClusterAccessor accessor = connection.createClusterAccessor(clusterId);
    UserConfig userConfig = resource.getUserConfig();
    Map<String, String> mapField = Maps.newHashMap();
    mapField.put("k1", "v1");
    mapField.put("k2", "v2");
    userConfig.setMapField("sampleMap", mapField);
    ResourceConfig.Delta delta = new ResourceConfig.Delta(resource.getId()).addUserConfig(userConfig);
    accessor.updateResource(resource.getId(), delta);
  }

  private static void createCluster(ClusterConfig cluster, HelixConnection connection) {
    ClusterAccessor accessor = connection.createClusterAccessor(cluster.getId());
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
        new ParticipantConfig.Builder(participantId).hostName("localhost").port(0).userConfig(userConfig);
    return participantBuilder.build();
  }

  private static ResourceConfig getResource(StateModelDefinition stateModelDef) {
    // identify the resource
    ResourceId resourceId = ResourceId.from("exampleResource");

    // create a partition
    PartitionId partition1 = PartitionId.from(resourceId, "1");

    // create a second partition
    PartitionId partition2 = PartitionId.from(resourceId, "2");

    // specify the ideal state
    // this resource will be rebalanced in FULL_AUTO mode, so use the AutoRebalanceModeISBuilder
    // builder
    AutoRebalanceModeISBuilder idealStateBuilder =
        new AutoRebalanceModeISBuilder(resourceId).add(partition1).add(partition2);
    idealStateBuilder.setNumReplica(1).setStateModelDefId(stateModelDef.getStateModelDefId());

    // create (optional) user-defined configuration properties for the resource
    UserConfig userConfig = new UserConfig(Scope.resource(resourceId));
    userConfig.setBooleanField("sampleBoolean", true);

    // create the configuration for a new resource
    ResourceConfig.Builder resourceBuilder =
        new ResourceConfig.Builder(resourceId).idealState(idealStateBuilder.build()).userConfig(userConfig);
    return resourceBuilder.build();
  }

  private static StateModelDefinition getLockUnlockModel() {
    final State LOCKED = State.from("LOCKED");
    final State RELEASED = State.from("RELEASED");
    final State DROPPED = State.from("DROPPED");
    StateModelDefId stateModelId = StateModelDefId.from("LockUnlock");
    StateModelDefinition.Builder stateModelBuilder =
        new StateModelDefinition.Builder(stateModelId).addState(LOCKED, 0).addState(RELEASED, 1).addState(DROPPED, 2)
            .addTransition(RELEASED, LOCKED, 0).addTransition(LOCKED, RELEASED, 1).addTransition(RELEASED, DROPPED, 2)
            .upperBound(LOCKED, 1).upperBound(RELEASED, -1).upperBound(DROPPED, -1).initialState(RELEASED);
    return stateModelBuilder.build();
  }

  /**
   * Dummy state model that just prints state transitions for the lock-unlock model
   */
  @StateModelInfo(initialState = "OFFLINE", states = { "LOCKED", "RELEASED", "DROPPED", "ERROR" })
  public static class LockUnlockStateModel extends TransitionHandler {
    private final PartitionId _partitionId;

    /**
     * Instantiate for a partition
     * @param partitionId the partition for which to track state transitions
     */
    public LockUnlockStateModel(PartitionId partitionId) {
      _partitionId = partitionId;
    }

    public void onBecomeLockedFromReleased(Message message, NotificationContext context) {
      onBecomeAnyFromAny(message, context);
    }

    public void onBecomeReleasedFromLocked(Message message, NotificationContext context) {
      onBecomeAnyFromAny(message, context);
    }

    public void onBecomeDroppedFromReleased(Message message, NotificationContext context) {
      onBecomeAnyFromAny(message, context);
    }

    public void onBecomeAnyFromAny(Message message, NotificationContext context) {
      System.out.println("Partition " + _partitionId + " transition from " + message.getFromState() + " to "
          + message.getToState());
    }
  }

  /**
   * State model factory for lock-unlock
   */
  public static class LockUnlockFactory extends StateTransitionHandlerFactory<LockUnlockStateModel> {
    @Override
    public LockUnlockStateModel createStateTransitionHandler(ResourceId resource, PartitionId partitionId) {
      return new LockUnlockStateModel(partitionId);
    }
  }
}
