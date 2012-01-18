package com.linkedin.clustermanager.controller.stages;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.model.StateModelDefinition;

/**
 * 
 * order the messages by stateTransitionPriority for each message take the
 * current state array Ii,Si Get new state after apply if this valid store it
 * 
 * allMessageList; selectedMessages; for(msg in allMessages)
 * selectedMessages.add(msg) if(!checkConstraintForAllCombinations(currentstate,
 * selectedMessages)){ //remove message since it violates the constraints
 * selectedMessages.remove(msg) } }
 * 
 * @author kgopalak
 * 
 */
public class MessageSelectionAlgo
{
  private static Logger logger = Logger.getLogger(MessageSelectionAlgo.class);

  /**
   * Assumes the messages are sorted according to priority. This returns the max
   * number of messages that can be applied without violating the state model
   * constraints
   * 
   * @param messages
   * @param stateModelDefinition
   * @return
   */
  List<Message> selectMessages(ResourceGroup resourceGroup,
      ResourceKey resource, List<Message> messages,
      StateModelDefinition stateModelDefinition,
      CurrentStateOutput currentState, IdealState idealState,
      ClusterDataCache cache)
  {

    LinkedList<Message> validMessages = new LinkedList<Message>();

    // Go over all instances current state instead of idealstate preference list
    // because
    // if there was a rebalance done then some resource might exist in current
    // state but not in idealstate
    // Also it might be custom mode in which case preference list is empty. But
    // we need throttling/message selection irrespective of idealstate mode

    Map<String, LiveInstance> liveInstancesMap = cache.getLiveInstances();
    Map<String, String> currentStateMap = new HashMap<String, String>();
    Map<String, String> pendingStateMap = new HashMap<String, String>();

    for (String instanceName : liveInstancesMap.keySet())
    {
      String instanceCurrentState = currentState.getCurrentState(
          resourceGroup.getResourceGroupId(), resource, instanceName);
      if (instanceCurrentState != null)
      {
        currentStateMap.put(instanceName, instanceCurrentState);
      }
      String instancePendingState = currentState.getPendingState(
          resourceGroup.getResourceGroupId(), resource, instanceName);
      if (instancePendingState != null)
      {
        pendingStateMap.put(instanceName, instancePendingState);
      }
    }

    // this will hold all possible states
    Set<ResourceStateMap> possibleStates = new HashSet<ResourceStateMap>();
    // add the current state
    possibleStates.add(new ResourceStateMap(currentStateMap));
    Set<ResourceStateMap> newPossibleStates = new HashSet<MessageSelectionAlgo.ResourceStateMap>();
    for (String instance : pendingStateMap.keySet())
    {
      for (ResourceStateMap resourceStateMap : possibleStates)
      {
        ResourceStateMap newPossibleState = ResourceStateMap.build(
            resourceStateMap, instance, pendingStateMap.get(instance));
        newPossibleStates.add(newPossibleState);
      }
    }
    possibleStates.addAll(newPossibleStates);

    for (int i = 0; i < messages.size(); i++)
    {
      Message msg = messages.get(i);
      validMessages.add(msg);
      newPossibleStates.clear();
      for (ResourceStateMap resourceStateMap : possibleStates)
      {
        ResourceStateMap newPossibleState = ResourceStateMap.build(
            resourceStateMap, msg.getTgtName(), msg.getToState());
        newPossibleStates.add(newPossibleState);
      }

      boolean isValid = validateNewPossibleStates(newPossibleStates,
          stateModelDefinition, idealState, cache);
      // if sending this message has the possibility to violate state constraint
      // don't send it
      if (!isValid)
      {
        validMessages.removeLast();
        // if any message violates the constraint and we have at least one
        // message to send we don't have to continue checking further
        if (validMessages.size() > 0)
        {
          break;
        }
      }
    }
    logger.info("Message selection algo selected " + validMessages.size()
        + " out of " + messages.size());
    return validMessages;
  }

  /**
   * The goal of this method is to validate the overall state of the cluster
   * when messages are being processed by the nodes. The challenge here is
   * messages can be performed in any order and we need to ensure all
   * permutations result in the valid state
   * 
   * @param states
   * @param stateModelDefinition
   * @param idealState
   * @return
   */
  private boolean validateNewPossibleStates(Set<ResourceStateMap> states,
      StateModelDefinition stateModelDefinition, IdealState idealState,
      ClusterDataCache cache)
  {

    Map<String, Integer> maxInstancePerStateMap = computeMaxInstanceAllowedPerState(
        stateModelDefinition, idealState, cache);

    boolean valid = true;
    for (ResourceStateMap stateSet : states)
    {
      if (!isValid(stateSet, maxInstancePerStateMap))
      {
        valid = false;
        break;
      }
    }
    return valid;
  }

  private Map<String, Integer> computeMaxInstanceAllowedPerState(
      StateModelDefinition stateModelDefinition, IdealState idealState,
      ClusterDataCache cache)
  {
    Map<String, Integer> maxInstancePerStateMap = new HashMap<String, Integer>();
    List<String> statePriorityList = stateModelDefinition
        .getStatesPriorityList();
    for (String state : statePriorityList)
    {
      String numInstancesPerState = stateModelDefinition
          .getNumInstancesPerState(state);
      int max;
      if ("N".equals(numInstancesPerState))
      {
        max = cache.getLiveInstances().size();
      } else if ("R".equals(numInstancesPerState))
      {
        max = idealState.getReplicas();
      } else
      {
        try
        {
          max = Integer.parseInt(numInstancesPerState);
        } catch (Exception e)
        {
          max = -1;
        }
      }
      if (max > -1)
      {
        maxInstancePerStateMap.put(state, max);
      }
    }
    return maxInstancePerStateMap;
  }

  private boolean isValid(ResourceStateMap stateSet,
      Map<String, Integer> maxInstancePerStateMap)
  {
    boolean valid = true;
    for (String state : maxInstancePerStateMap.keySet())
    {
      if (stateSet.getStateCount(state) > maxInstancePerStateMap.get(state))
      {
        valid = false;
        break;
      }
    }
    return valid;
  }

  /**
   * 
   * Immutable Tuple consisting of Instance,State Pairs Denotes the state of a
   * resource/partition at various instances
   * 
   * @author kgopalak
   * 
   */
  static class ResourceStateMap
  {

    private final TreeMap<String, String> _map;
    private Map<String, Integer> stateCountMap = new HashMap<String, Integer>();
    private String _toString;

    public ResourceStateMap(Map<String, String> map)
    {
      _map = new TreeMap<String, String>(map);
      for (String state : map.values())
      {
        Integer count = stateCountMap.get(state);
        if (count == null)
        {
          count = new Integer(1);
        } else
        {
          count = count + 1;
        }
        stateCountMap.put(state, count);
      }
      _toString = _map.toString();
    }

    public static ResourceStateMap build(ResourceStateMap resourceStateMap,
        String instance, String state)
    {
      TreeMap<String, String> map = new TreeMap<String, String>(
          resourceStateMap._map);
      map.put(instance, state);
      return new ResourceStateMap(map);
    }

    int getStateCount(String state)
    {
      return stateCountMap.containsKey(state) ? stateCountMap.get(state) : 0;
    }

    @Override
    public int hashCode()
    {
      return _toString.hashCode();
    }

    @Override
    public String toString()
    {
      return _toString;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj instanceof ResourceStateMap)
      {
        ResourceStateMap that = (ResourceStateMap) obj;
        return this.toString().equals(that.toString());
      }
      return false;
    }
  }
}
