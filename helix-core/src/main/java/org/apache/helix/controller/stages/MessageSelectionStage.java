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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Resource;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

public class MessageSelectionStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(MessageSelectionStage.class);

  public static class Bounds {
    private int upper;
    private int lower;

    public Bounds(int lower, int upper) {
      this.lower = lower;
      this.upper = upper;
    }

    public void increaseUpperBound() {
      upper++;
    }

    public void increaseLowerBound() {
      lower++;
    }

    public void decreaseUpperBound() {
      upper--;
    }

    public void decreaseLowerBound() {
      lower--;
    }

    public int getLowerBound() {
      return lower;
    }

    public int getUpperBound() {
      return upper;
    }

    @Override
    public String toString() {
      return String.format("%d-%d", lower, upper);
    }
  }

  @Override
  public void process(ClusterEvent event) throws Exception {
    Cluster cluster = event.getAttribute("Cluster");
    Map<StateModelDefId, StateModelDefinition> stateModelDefMap = cluster.getStateModelMap();
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    ResourceCurrentState currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    MessageOutput messageGenOutput = event.getAttribute(AttributeName.MESSAGES_ALL.toString());
    if (cluster == null || resourceMap == null || currentStateOutput == null
        || messageGenOutput == null || bestPossibleStateOutput == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires Cluster|RESOURCES|CURRENT_STATE|BEST_POSSIBLE_STATE|MESSAGES_ALL");
    }

    MessageOutput output = new MessageOutput();

    for (ResourceId resourceId : resourceMap.keySet()) {
      ResourceConfig resource = resourceMap.get(resourceId);
      StateModelDefinition stateModelDef =
          stateModelDefMap.get(resource.getIdealState().getStateModelDefId());

      if (stateModelDef == null) {
        LOG.info("resource: "
            + resourceId
            + " doesn't have state-model-def; e.g. we add a resource config but not add the resource in ideal-states");
        continue;
      }

      // TODO have a logical model for transition
      Map<String, Integer> stateTransitionPriorities = getStateTransitionPriorityMap(stateModelDef);
      Resource configResource = cluster.getResource(resourceId);

      // if configResource == null, the resource has been dropped
      Map<State, Bounds> stateConstraints =
          computeStateConstraints(stateModelDef,
              configResource == null ? null : configResource.getIdealState(), cluster);

      // TODO fix it
      for (PartitionId partitionId : bestPossibleStateOutput.getResourceAssignment(resourceId)
          .getMappedPartitionIds()) {
        List<Message> messages = messageGenOutput.getMessages(resourceId, partitionId);
        List<Message> selectedMessages =
            selectMessages(cluster.getLiveParticipantMap(),
                currentStateOutput.getCurrentStateMap(resourceId, partitionId),
                currentStateOutput.getPendingStateMap(resourceId, partitionId), messages,
                stateConstraints, stateTransitionPriorities, stateModelDef.getTypedInitialState());
        output.setMessages(resourceId, partitionId, selectedMessages);
      }
    }
    event.addAttribute(AttributeName.MESSAGES_SELECTED.toString(), output);
  }

  // TODO: This method deserves its own class. The class should not understand helix but
  // just be
  // able to solve the problem using the algo. I think the method is following that but if
  // we don't move it to another class its quite easy to break that contract
  /**
   * greedy message selection algorithm: 1) calculate CS+PS state lower/upper-bounds 2)
   * group messages by state transition and sorted by priority 3) from highest priority to
   * lowest, for each message group with the same transition add message one by one and
   * make sure state constraint is not violated update state lower/upper-bounds when a new
   * message is selected
   * @param currentStates
   * @param pendingStates
   * @param messages
   * @param stateConstraints
   *          : STATE -> bound (lower:upper)
   * @param stateTransitionPriorities
   *          : FROME_STATE-TO_STATE -> priority
   * @return: selected messages
   */
  List<Message> selectMessages(Map<ParticipantId, Participant> liveParticipants,
      Map<ParticipantId, State> currentStates, Map<ParticipantId, State> pendingStates,
      List<Message> messages, Map<State, Bounds> stateConstraints,
      final Map<String, Integer> stateTransitionPriorities, State initialState) {
    if (messages == null || messages.isEmpty()) {
      return Collections.emptyList();
    }

    List<Message> selectedMessages = new ArrayList<Message>();
    Map<State, Bounds> bounds = new HashMap<State, Bounds>();

    // count currentState, if no currentState, count as in initialState
    for (ParticipantId liveParticipantId : liveParticipants.keySet()) {
      State state = initialState;
      if (currentStates.containsKey(liveParticipantId)) {
        state = currentStates.get(liveParticipantId);
      }

      if (!bounds.containsKey(state)) {
        bounds.put(state, new Bounds(0, 0));
      }
      bounds.get(state).increaseLowerBound();
      bounds.get(state).increaseUpperBound();
    }

    // count pendingStates
    for (ParticipantId participantId : pendingStates.keySet()) {
      State state = pendingStates.get(participantId);
      if (!bounds.containsKey(state)) {
        bounds.put(state, new Bounds(0, 0));
      }
      // TODO: add lower bound, need to refactor pendingState to include fromState also
      bounds.get(state).increaseUpperBound();
    }

    // group messages based on state transition priority
    Map<Integer, List<Message>> messagesGroupByStateTransitPriority =
        new TreeMap<Integer, List<Message>>();
    for (Message message : messages) {
      State fromState = message.getTypedFromState();
      State toState = message.getTypedToState();
      String transition = fromState.toString() + "-" + toState.toString();
      int priority = Integer.MAX_VALUE;

      if (stateTransitionPriorities.containsKey(transition)) {
        priority = stateTransitionPriorities.get(transition);
      }

      if (!messagesGroupByStateTransitPriority.containsKey(priority)) {
        messagesGroupByStateTransitPriority.put(priority, new ArrayList<Message>());
      }
      messagesGroupByStateTransitPriority.get(priority).add(message);
    }

    // select messages
    for (List<Message> messageList : messagesGroupByStateTransitPriority.values()) {
      for (Message message : messageList) {
        State fromState = message.getTypedFromState();
        State toState = message.getTypedToState();

        if (!bounds.containsKey(fromState)) {
          LOG.error("Message's fromState is not in currentState. message: " + message);
          continue;
        }

        if (!bounds.containsKey(toState)) {
          bounds.put(toState, new Bounds(0, 0));
        }

        // check lower bound of fromState
        if (stateConstraints.containsKey(fromState)) {
          int newLowerBound = bounds.get(fromState).getLowerBound() - 1;
          if (newLowerBound < 0) {
            LOG.error("Number of currentState in " + fromState
                + " is less than number of messages transiting from " + fromState);
            continue;
          }

          if (newLowerBound < stateConstraints.get(fromState).getLowerBound()) {
            continue;
          }
        }

        // check upper bound of toState
        if (stateConstraints.containsKey(toState)) {
          int newUpperBound = bounds.get(toState).getUpperBound() + 1;
          if (newUpperBound > stateConstraints.get(toState).getUpperBound()) {
            continue;
          }
        }

        selectedMessages.add(message);
        bounds.get(fromState).increaseLowerBound();
        bounds.get(toState).increaseUpperBound();
      }
    }

    return selectedMessages;
  }

  /**
   * TODO: This code is duplicate in multiple places. Can we do it in to one place in the
   * beginning and compute the stateConstraint instance once and re use at other places.
   * Each IdealState must have a constraint object associated with it
   * @param stateModelDefinition
   * @param rebalancerConfig if rebalancerConfig == null, we can't evaluate R thus no constraints
   * @param cluster
   * @return
   */
  private Map<State, Bounds> computeStateConstraints(StateModelDefinition stateModelDefinition,
      IdealState idealState, Cluster cluster) {
    Map<State, Bounds> stateConstraints = new HashMap<State, Bounds>();

    List<State> statePriorityList = stateModelDefinition.getTypedStatesPriorityList();
    for (State state : statePriorityList) {
      String numInstancesPerState = stateModelDefinition.getNumParticipantsPerState(state);
      int max = -1;
      if ("N".equals(numInstancesPerState)) {
        max = cluster.getLiveParticipantMap().size();
      } else if ("R".equals(numInstancesPerState)) {
        // idealState is null when resource has been dropped,
        // R can't be evaluated and ignore state constraints
        if (idealState != null) {
          String replicas = idealState.getReplicas();
          if (replicas.equals(StateModelToken.ANY_LIVEINSTANCE.toString())) {
            max = cluster.getLiveParticipantMap().size();
          } else {
            max = Integer.parseInt(replicas);
          }
        }
      } else {
        try {
          max = Integer.parseInt(numInstancesPerState);
        } catch (Exception e) {
          // use -1
        }
      }

      if (max > -1) {
        // if state has no constraint, will not put in map
        stateConstraints.put(state, new Bounds(0, max));
      }
    }

    return stateConstraints;
  }

  // TODO: if state transition priority is not provided then use lexicographical sorting
  // so that behavior is consistent
  private Map<String, Integer> getStateTransitionPriorityMap(StateModelDefinition stateModelDef) {
    Map<String, Integer> stateTransitionPriorities = new HashMap<String, Integer>();
    List<String> stateTransitionPriorityList = stateModelDef.getStateTransitionPriorityList();
    for (int i = 0; i < stateTransitionPriorityList.size(); i++) {
      stateTransitionPriorities.put(stateTransitionPriorityList.get(i), i);
    }

    return stateTransitionPriorities;
  }
}
