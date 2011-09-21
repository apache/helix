package com.linkedin.clustermanager.controller;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.StateModelDefinition;

public class TransitionMessageGenerator
{
  private static Logger logger = Logger
      .getLogger(TransitionMessageGenerator.class);
  private final StateModelDefinition _stateModelDefinition;
  private final CurrentStateHolder _currentStateHolder;
  private final MessageHolder _messageHolder;
  private final LiveInstanceDataHolder _liveInstanceDataHolder;
  private final InstanceConfigHolder _instanceConfigHolder;

  public TransitionMessageGenerator(StateModelDefinition stateModelDefinition,
      CurrentStateHolder currentStateHolder, MessageHolder messageHolder,
      LiveInstanceDataHolder liveInstanceDataHolder,
      InstanceConfigHolder instanceConfigHolder)
  {
    this._stateModelDefinition = stateModelDefinition;
    this._currentStateHolder = currentStateHolder;
    this._messageHolder = messageHolder;
    this._liveInstanceDataHolder = liveInstanceDataHolder;
    this._instanceConfigHolder = instanceConfigHolder;
  }

  /**
   * @param idealState
   * @param bestPossibleIdealState
   * @param idealStateRecord
   * @return
   */
  public List<Message> computeMessagesForTransition(IdealState idealState,
      IdealState bestPossibleIdealState, ZNRecord idealStateRecord)
  {

    List<Message> messages = new ArrayList<Message>();
    for (String stateUnitKey : idealState.stateUnitSet())
    {
      Map<String, String> instanceStateMap;
      instanceStateMap = idealState.getInstanceStateMap(stateUnitKey);
      for (String instanceName : instanceStateMap.keySet())
      {
        if (!_liveInstanceDataHolder.isAlive(instanceName))
        {
          logger.info(instanceName + " is down ");
          continue;
        } else if (!_instanceConfigHolder.isEnabled(instanceName))
        {
          logger.info(instanceName + " is disabled ");
          continue;
        }
        String currentState = _currentStateHolder.getState(stateUnitKey,
            instanceName);
        if (currentState == null)
        {
          currentState = _stateModelDefinition.getInitialState();
        }
        String desiredState = bestPossibleIdealState.get(stateUnitKey,
            instanceName);
        // logger.info("Ideal state for " + stateUnitKey + " - "
        // + instanceName + "  is " + desiredState);
        if (desiredState == null
            && !_liveInstanceDataHolder.isAlive(instanceName))
        {
          // log this only if instance is not down
          logger.warn("Cannot find ideal state for key:" + stateUnitKey
              + " instance:" + instanceName);
          continue;
        }
        String pendingState = _messageHolder.get(stateUnitKey, instanceName);
        if (!desiredState.equalsIgnoreCase(currentState))
        {

          String nextState;
          nextState = _stateModelDefinition.getNextStateForTransition(
              currentState, desiredState);
          if (nextState != null)
          {
            if (pendingState != null
                && nextState.equalsIgnoreCase(pendingState))
            {
              logger.info("Message already exists to transition from "
                  + currentState + " to " + nextState);
            } else
            {
              Message message = createMessage(idealStateRecord, stateUnitKey,
                  instanceName, currentState, nextState);
              messages.add(message);
            }
          } else
          {
            logger.warn("Unable to find a next state from stateModelDefinition"
                + _stateModelDefinition.getClass() + " from:" + currentState
                + " to:" + idealState);
          }
        }
      }
    }

    return messages;
  }

  private Message createMessage(ZNRecord idealStateRecord, String stateUnitKey,
      String instanceName, String currentState, String nextState)
  {
    Message message = new Message(Message.MessageType.STATE_TRANSITION);
    String uuid = UUID.randomUUID().toString();
    message.setId(uuid);
    message.setMsgId(uuid);
    String hostName = "UNKNOWN";
    try
    {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e)
    {
      logger.info(
          "Unable to get Host name. Will set it to UNKNOWN, mostly ignorable",
          e);
      // can ignore it,
    }
    message.setSrcName(hostName);
    message.setTgtName(instanceName);
    message.setMsgState("new");
    message.setStateUnitKey(stateUnitKey);
    message.setStateUnitGroup(idealStateRecord.getId());
    message.setFromState(currentState);
    message.setToState(nextState);

    String sessionId = _liveInstanceDataHolder.getSessionId(instanceName);

    message.setTgtSessionId(sessionId);
    message.setSrcSessionId(sessionId);
    return message;
  }
}
