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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixException;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSelectionStage extends AbstractBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(MessageSelectionStage.class);

  protected static class Bounds {
    private int upper;
    private int lower;

    public Bounds(int lower, int upper) {
      this.lower = lower;
      this.upper = upper;
    }

    public int getUpperBound() {
      return upper;
    }
  }

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    ResourceControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());

    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.name());
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    MessageOutput messageGenOutput =
        event.getAttribute(AttributeName.MESSAGES_ALL.name());
    if (cache == null || resourceMap == null || currentStateOutput == null
        || messageGenOutput == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires DataCache|RESOURCES|CURRENT_STATE|MESSAGES_ALL");
    }

    MessageOutput output = new MessageOutput();

    for (String resourceName : resourceMap.keySet()) {
      Resource resource = resourceMap.get(resourceName);
      try {
        StateModelDefinition stateModelDef = cache.getStateModelDef(resource.getStateModelDefRef());
        Map<String, Integer> stateTransitionPriorities = getStateTransitionPriorityMap(stateModelDef);
        IdealState idealState = cache.getIdealState(resourceName);
        Map<String, Bounds> stateConstraints =
            computeStateConstraints(stateModelDef, idealState, cache);
        for (Partition partition : resource.getPartitions()) {
          List<Message> messages = messageGenOutput.getMessages(resourceName, partition);
          List<Message> selectedMessages = selectMessages(cache.getLiveInstances(),
              currentStateOutput.getCurrentStateMap(resourceName, partition),
              currentStateOutput.getPendingMessageMap(resourceName, partition), messages,
              currentStateOutput.getPendingRelayMessageMap(resourceName, partition).values(),
              stateConstraints, stateTransitionPriorities, stateModelDef,
              resource.isP2PMessageEnabled());
          output.addMessages(resourceName, partition, selectedMessages);
        }
      } catch (HelixException ex) {
        LogUtil.logError(LOG, _eventId,
            "Failed to finish message selection for resource " + resourceName, ex);
      }
    }
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), output);
  }

  private void increaseStateCnt(Map<String, Bounds> stateConstraints, String state,
      Map<String, Integer> stateCnts) {
    if (!stateConstraints.containsKey(state)) {
      // skip state that doesn't have constraint
      return;
    }
    if (!stateCnts.containsKey(state)) {
      stateCnts.put(state, 0);
    }
    stateCnts.put(state, stateCnts.get(state) + 1);
  }

  // TODO: This method deserves its own class. The class should not understand helix but
  // just be able to solve the problem using the algo. I think the method is following that
  // but if we don't move it to another class its quite easy to break that contract
  /**
   * greedy message selection algorithm: 1) calculate CS+PS state lower/upper-bounds 2)
   * group messages by state transition and sorted by priority 3) from highest priority to
   * lowest, for each message group with the same transition add message one by one and
   * make sure state constraint is not violated update state lower/upper-bounds when a new
   * message is selected.
   *
   * @param liveInstances
   * @param currentStates
   * @param pendingMessages
   * @param messages
   * @param stateConstraints
   * @param stateTransitionPriorities
   * @param stateModelDef
   * @return
   */
  List<Message> selectMessages(Map<String, LiveInstance> liveInstances,
      Map<String, String> currentStates, Map<String, Message> pendingMessages,
      List<Message> messages, Collection<Message> pendingRelayMessages,
      Map<String, Bounds> stateConstraints, final Map<String, Integer> stateTransitionPriorities,
      StateModelDefinition stateModelDef, boolean p2pMessageEnabled) {
    if (messages == null || messages.isEmpty()) {
      return Collections.emptyList();
    }
    List<Message> selectedMessages = new ArrayList<>();
    Map<String, Integer> stateCnts = new HashMap<>();

    String initialState = stateModelDef.getInitialState();
    // count currentState, if no currentState, count as in initialState
    for (String instance : liveInstances.keySet()) {
      String state = initialState;
      if (currentStates.containsKey(instance)) {
        state = currentStates.get(instance);
      }

      increaseStateCnt(stateConstraints, state, stateCnts);
    }

    // count pendingStates
    for (String instance : pendingMessages.keySet()) {
      Message message = pendingMessages.get(instance);
      increaseStateCnt(stateConstraints, message.getToState(), stateCnts);
      increaseStateCnt(stateConstraints, message.getFromState(), stateCnts);
    }

    // group messages based on state transition priority
    Map<Integer, List<Message>> messagesGroupByStateTransitPriority =
        new TreeMap<>();

    /* record all state transition messages that transition a replica from top-state */
    List<Message> fromTopStateMessages = new LinkedList<>();
    for (Message message : messages) {
      if (message.getMsgType().equals(Message.MessageType.STATE_TRANSITION_CANCELLATION.name())) {
        selectedMessages.add(message);
        continue;
      }
      String fromState = message.getFromState();
      String toState = message.getToState();
      String transition = fromState + "-" + toState;
      int priority = Integer.MAX_VALUE;

      if (stateTransitionPriorities.containsKey(transition)) {
        priority = stateTransitionPriorities.get(transition);
      }

      if (!messagesGroupByStateTransitPriority.containsKey(priority)) {
        messagesGroupByStateTransitPriority.put(priority, new ArrayList<>());
      }
      messagesGroupByStateTransitPriority.get(priority).add(message);

      if (fromState.equals(stateModelDef.getTopState())) {
        fromTopStateMessages.add(message);
      }
    }

    // select messages
    for (List<Message> messageList : messagesGroupByStateTransitPriority.values()) {
      NextMessage:
      for (Message message : messageList) {
        String toState = message.getToState();
        String fromState = message.getFromState();

        if (toState.equals(stateModelDef.getTopState())) {
          // find if there are any pending relay messages match this message.
          // if the pending relay message targets the same host, we are fine to continue send the message,
          // if it targets to different host, we should not send the message now (should send after the relay message gets expired).
          for (Message relayMsg : pendingRelayMessages) {
            if (relayMsg.getToState().equals(toState) && relayMsg.getFromState()
                .equals(fromState)) {
              LOG.info(
                  "There is pending relay message, pending relay message: {}, relay time starts {}, expiry timeout {}.",
                  relayMsg.getMsgId(), relayMsg.getRelayTime(), relayMsg.getExpiryPeriod());
              if (!relayMsg.getTgtName().equals(message.getTgtName())) {
                LOG.info(
                    "The pending relay message was sent to a different host, not send message: {}, pending relay message: {}",
                    message.getMsgId(), relayMsg.getId());
                continue NextMessage;
              }
            }
          }
        }

        if (stateConstraints.containsKey(toState)) {
          int newCnt = (stateCnts.containsKey(toState) ? stateCnts.get(toState) + 1 : 1);
          if (newCnt > stateConstraints.get(toState).getUpperBound()) {
            if (p2pMessageEnabled && toState.equals(stateModelDef.getTopState())
                && stateModelDef.isSingleTopStateModel()) {
              // attach this message as a relay message to the message to transition off current top-state replica
              if (fromTopStateMessages.size() > 0) {
                Message fromTopStateMsg = fromTopStateMessages.get(0);
                fromTopStateMsg.attachRelayMessage(message.getTgtName(), message);
                fromTopStateMessages.remove(0);
              }
            } else {
              // reach upper-bound of message for the topState, will not send the message
              LogUtil.logInfo(LOG, _eventId,
                  "Reach upper_bound: " + stateConstraints.get(toState).getUpperBound()
                      + ", not send message: " + message);
            }
            continue;
          }
        }

        increaseStateCnt(stateConstraints, message.getToState(), stateCnts);
        selectedMessages.add(message);
      }
    }

    return selectedMessages;
  }

  /**
   * TODO: This code is duplicate in multiple places. Can we do it in to one place in the
   * beginning and compute the stateConstraint instance once and re use at other places.
   * Each IdealState must have a constraint object associated with it
   */
  private Map<String, Bounds> computeStateConstraints(StateModelDefinition stateModelDefinition,
      IdealState idealState, ResourceControllerDataProvider cache) {
    Map<String, Bounds> stateConstraints = new HashMap<String, Bounds>();

    List<String> statePriorityList = stateModelDefinition.getStatesPriorityList();
    for (String state : statePriorityList) {
      String numInstancesPerState = stateModelDefinition.getNumInstancesPerState(state);
      int max = -1;
      if ("N".equals(numInstancesPerState)) {
        max = cache.getLiveInstances().size();
      } else if ("R".equals(numInstancesPerState)) {
        // idealState is null when resource has been dropped,
        // R can't be evaluated and ignore state constraints
        //if (idealState != null) {
        //  max = cache.getReplicas(idealState.getResourceName());
        //}
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
