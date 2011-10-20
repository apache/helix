package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.IdealState;
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
      CurrentStateOutput currentState, IdealState idealState)
  {
    LinkedList<Message> validMessages = new LinkedList<Message>();

    List<String> instancePreferenceList = idealState.getPreferenceList(
        resource.getResourceKeyName(), stateModelDefinition);
    Map<String, String> currentStateMap = new HashMap<String, String>();
    Map<String, String> pendingStateMap = new HashMap<String, String>();
    for (String instanceName : instancePreferenceList)
    {
      String instanceCurrentState = currentState.getCurrentState(
          resourceGroup.getResourceGroupId(), resource, instanceName);
      currentStateMap.put(instanceName, instanceCurrentState);

      String instancePendingState = currentState.getPendingState(
          resourceGroup.getResourceGroupId(), resource, instanceName);
      if (instancePendingState != null)
      {
        pendingStateMap.put(instanceName, instancePendingState);
      }
    }
    for (int i = 0; i < messages.size(); i++)
    {
      validMessages.add(messages.get(i));
      boolean isValid = checkConstraintForAllCombinations(validMessages,
          currentStateMap, pendingStateMap, stateModelDefinition);
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
   * The goal of this method is to validate the overall state of the cluster when messages are being processed by the nodes.
   * The challenge here is messages can be performed in any order and we need to ensure all permutations result in the valid state
   * @param validMessages
   * @param currentStateMap
   * @param pendingStateMap
   * @param stateModelDefinition
   * @return
   */
  private boolean checkConstraintForAllCombinations(
      LinkedList<Message> validMessages, Map<String, String> currentStateMap,
      Map<String, String> pendingStateMap,
      StateModelDefinition stateModelDefinition)
  {
    
    return false;
  }
}
