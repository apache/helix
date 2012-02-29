package com.linkedin.helix.controller.stages;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.StateModelDefinition;

/**
 * Compares the currentState,pendingState with IdealState and generate messages
 *
 * @author kgopalak
 *
 */
public class MessageGenerationPhase extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(MessageGenerationPhase.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    HelixManager manager = event.getAttribute("helixmanager");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    Map<String, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());
    CurrentStateOutput currentStateOutput = event.getAttribute(AttributeName.CURRENT_STATE
        .toString());
    BestPossibleStateOutput bestPossibleStateOutput = event
        .getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    if (manager == null || cache == null || resourceMap == null || currentStateOutput == null
        || bestPossibleStateOutput == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|DataCache|RESOURCES|CURRENT_STATE|BEST_POSSIBLE_STATE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    Map<String, String> sessionIdMap = new HashMap<String, String>();

    for (LiveInstance liveInstance : liveInstances.values())
    {
      sessionIdMap.put(liveInstance.getInstanceName(), liveInstance.getSessionId());
    }
    MessageGenerationOutput output = new MessageGenerationOutput();

    for (String resourceName : resourceMap.keySet())
    {
      Resource resource = resourceMap.get(resourceName);
      StateModelDefinition stateModelDef = cache.getStateModelDef(resource.getStateModelDefRef());

      for (Partition partition : resource.getPartitions())
      {
        Map<String, String> instanceStateMap = bestPossibleStateOutput.getInstanceStateMap(
            resourceName, partition);

        for (String instanceName : instanceStateMap.keySet())
        {
          String desiredState = instanceStateMap.get(instanceName);

          String currentState = currentStateOutput.getCurrentState(resourceName, partition,
              instanceName);
          if (currentState == null)
          {
            currentState = stateModelDef.getInitialState();
          }

          String pendingState = currentStateOutput.getPendingState(resourceName, partition,
              instanceName);

          String nextState;
          nextState = stateModelDef.getNextStateForTransition(currentState, desiredState);

          if (!desiredState.equalsIgnoreCase(currentState))
          {
            if (nextState != null)
            {
              if (pendingState != null && nextState.equalsIgnoreCase(pendingState))
              {
                if (logger.isDebugEnabled())
                {
                  logger.debug("Message already exists at" + instanceName
                      + " to transit " + partition.getPartitionName() + " from "
                      + currentState + " to " + nextState);
                }
              }
              else if (pendingState != null
                  && currentState.equalsIgnoreCase(pendingState))
              {
                logger.debug("Message hasn't been removed for " + instanceName
                    + " to transit" + partition.getPartitionName() + " to "
                    + pendingState);
              } else
              {
                Message message = createMessage(manager, resourceName,
                    partition.getPartitionName(), instanceName, currentState, nextState,
                    sessionIdMap.get(instanceName), stateModelDef.getId(),
                    resource.getStateModelFactoryname());

                output.addMessage(resourceName, partition, message);
              }
            } else
            {
              logger.error("Unable to find a next state from stateModelDefinition"
                  + stateModelDef.getClass() + " from:" + currentState + " to:" + desiredState);
            }
          }
        }
      }
    }
    event.addAttribute(AttributeName.MESSAGES_ALL.toString(), output);
  }

  private Message createMessage(HelixManager manager, String resourceName, String partitionName,
      String instanceName, String currentState, String nextState, String sessionId,
      String stateModelDefName, String stateModelFactoryName)
  {
    String uuid = UUID.randomUUID().toString();
    Message message = new Message(MessageType.STATE_TRANSITION, uuid);
    message.setSrcName(manager.getInstanceName());
    message.setTgtName(instanceName);
    message.setMsgState("new");
    message.setPartitionName(partitionName);
    message.setResourceName(resourceName);
    message.setFromState(currentState);
    message.setToState(nextState);
    message.setTgtSessionId(sessionId);
    message.setSrcSessionId(manager.getSessionId());
    message.setStateModelDef(stateModelDefName);
    message.setStateModelFactoryName(stateModelFactoryName);
    return message;
  }
}
