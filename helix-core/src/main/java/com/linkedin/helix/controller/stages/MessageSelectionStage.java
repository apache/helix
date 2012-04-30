package com.linkedin.helix.controller.stages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.StateModelDefinition;

public class MessageSelectionStage extends AbstractBaseStage
{
  private static final Logger LOG = Logger.getLogger(MessageSelectionStage.class);

  static class Bounds
  {
    int upper;
    int lower;

    public Bounds(int lower, int upper)
    {
      this.lower = lower;
      this.upper = upper;
    }
  }

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    MessageGenerationOutput messageGenOutput =
        event.getAttribute(AttributeName.MESSAGES_ALL.toString());
    if (cache == null || resourceMap == null || currentStateOutput == null
        || messageGenOutput == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires DataCache|RESOURCES|CURRENT_STATE|MESSAGES_ALL");
    }

    MessageSelectionStageOutput output = new MessageSelectionStageOutput();

    for (String resourceName : resourceMap.keySet())
    {
      Resource resource = resourceMap.get(resourceName);
      StateModelDefinition stateModelDef =
          cache.getStateModelDef(resource.getStateModelDefRef());

      Map<String, Integer> stateTransitionPriorities =
          getStateTransitionPriorityMap(stateModelDef);
      IdealState idealState = cache.getIdealState(resourceName);
      Map<String, Bounds> stateConstraints =
          computeStateConstraints(stateModelDef, idealState, cache);

      for (Partition partition : resource.getPartitions())
      {
        List<Message> messages = messageGenOutput.getMessages(resourceName, partition);
        List<Message> selectedMessages =
            selectMessages(cache.getLiveInstances(),
                           currentStateOutput.getCurrentStateMap(resourceName, partition),
                           currentStateOutput.getPendingStateMap(resourceName, partition),
                           messages,
                           stateConstraints,
                           stateTransitionPriorities,
                           stateModelDef.getInitialState());
        output.addMessages(resourceName, partition, selectedMessages);
      }
    }
    event.addAttribute(AttributeName.MESSAGES_SELECTED.toString(), output);
  }

  /**
   * greedy message selection algorithm: 1) calculate CS+PS state lower/upper-bounds 2)
   * group messages by state transition and sorted by priority 3) from highest priority to
   * lowest, for each message group with the same transition add message one by one and
   * make sure state constraint is not violated update state lower/upper-bounds when a new
   * message is selected
   *
   * @param currentStates
   * @param pendingStates
   * @param messages
   * @param stateConstraints
   *          : STATE -> bound (lower:upper)
   * @param stateTransitionPriorities
   *          : FROME_STATE-TO_STATE -> priority
   * @return: selected messages
   */
  List<Message> selectMessages(Map<String, LiveInstance> liveInstances,
                               Map<String, String> currentStates,
                               Map<String, String> pendingStates,
                               List<Message> messages,
                               Map<String, Bounds> stateConstraints,
                               final Map<String, Integer> stateTransitionPriorities,
                               String initialState)
  {
    if (messages == null || messages.isEmpty())
    {
      return Collections.emptyList();
    }

    List<Message> selectedMessages = new ArrayList<Message>();
    Map<String, Bounds> bounds = new HashMap<String, Bounds>();

    // count currentState, if no currentState, count as in initialState
    for (String instance : liveInstances.keySet())
    {
      String state = initialState;
      if (currentStates.containsKey(instance))
      {
        state = currentStates.get(instance);
      }

      if (!bounds.containsKey(state))
      {
        bounds.put(state, new Bounds(0, 0));
      }
      bounds.get(state).lower++;
      bounds.get(state).upper++;
    }

    // count pendingStates
    for (String instance : pendingStates.keySet())
    {
      String state = pendingStates.get(instance);
      if (!bounds.containsKey(state))
      {
        bounds.put(state, new Bounds(0, 0));
      }
      // TODO: add lower bound, need to refactor pendingState to include fromState also
      bounds.get(state).upper++;
    }

    // group messages based on state transition priority
    Map<Integer, List<Message>> messagesGroupByStateTransitPriority =
        new TreeMap<Integer, List<Message>>();
    for (Message message : messages)
    {
      String fromState = message.getFromState();
      String toState = message.getToState();
      String transition = fromState + "-" + toState;
      int priority = Integer.MAX_VALUE;

      if (stateTransitionPriorities.containsKey(transition))
      {
        priority = stateTransitionPriorities.get(transition);
      }

      if (!messagesGroupByStateTransitPriority.containsKey(priority))
      {
        messagesGroupByStateTransitPriority.put(priority, new ArrayList<Message>());
      }
      messagesGroupByStateTransitPriority.get(priority).add(message);
    }

    for (List<Message> messageList : messagesGroupByStateTransitPriority.values())
    {
      for (Message message : messageList)
      {
        String fromState = message.getFromState();
        String toState = message.getToState();

        if (!bounds.containsKey(fromState))
        {
          LOG.error("Message's fromState is not in currentState. message: " + message);
          continue;
        }

        if (!bounds.containsKey(toState))
        {
          bounds.put(toState, new Bounds(0, 0));
        }

        // check lower bound of fromState
        if (stateConstraints.containsKey(fromState))
        {
          int newLowerBound = bounds.get(fromState).lower - 1;
          if (newLowerBound < 0)
          {
            LOG.error("Number of currentState in " + fromState
                + " is less than number of messages transiting from " + fromState);
            continue;
          }

          if (newLowerBound < stateConstraints.get(fromState).lower)
          {
            continue;
          }
        }

        // check upper bound of toState
        if (stateConstraints.containsKey(toState))
        {
          int newUpperBound = bounds.get(toState).upper + 1;
          if (newUpperBound > stateConstraints.get(toState).upper)
          {
            continue;
          }
        }

        selectedMessages.add(message);
        bounds.get(fromState).lower--;
        bounds.get(toState).upper++;
      }
    }

    return selectedMessages;
  }

  private Map<String, Bounds> computeStateConstraints(StateModelDefinition stateModelDefinition,
                                                      IdealState idealState,
                                                      ClusterDataCache cache)
  {
    Map<String, Bounds> stateConstraints = new HashMap<String, Bounds>();

    List<String> statePriorityList = stateModelDefinition.getStatesPriorityList();
    for (String state : statePriorityList)
    {
      String numInstancesPerState = stateModelDefinition.getNumInstancesPerState(state);
      int max = -1;
      if ("N".equals(numInstancesPerState))
      {
        max = cache.getLiveInstances().size();
      }
      else if ("R".equals(numInstancesPerState))
      {
        // idealState is null when resource has been dropped,
        // R can't be evaluated and ignore state constraints
        if (idealState != null)
        {
          max = cache.getReplicas(idealState.getResourceName());
        }
      }
      else
      {
        try
        {
          max = Integer.parseInt(numInstancesPerState);
        }
        catch (Exception e)
        {
          // use -1
        }
      }

      if (max > -1)
      {
        // if state has no constraint, will not put in map
        stateConstraints.put(state, new Bounds(0, max));
      }
    }

    return stateConstraints;
  }

  private Map<String, Integer> getStateTransitionPriorityMap(StateModelDefinition stateModelDef)
  {
    Map<String, Integer> stateTransitionPriorities = new HashMap<String, Integer>();
    List<String> stateTransitionPriorityList =
        stateModelDef.getStateTransitionPriorityList();
    for (int i = 0; i < stateTransitionPriorityList.size(); i++)
    {
      stateTransitionPriorities.put(stateTransitionPriorityList.get(i), i);
    }

    return stateTransitionPriorities;
  }
}
