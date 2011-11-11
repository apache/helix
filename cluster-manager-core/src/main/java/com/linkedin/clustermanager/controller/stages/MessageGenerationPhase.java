package com.linkedin.clustermanager.controller.stages;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;
/**
 * Compares the currentState,pendingState with IdealState and generate messages
 * @author kgopalak
 *
 */
public class MessageGenerationPhase extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(MessageGenerationPhase.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    CurrentStateOutput currentStateOutput = event
        .getAttribute(AttributeName.CURRENT_STATE.toString());
    BestPossibleStateOutput bestPossibleStateOutput = event
        .getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    if (manager == null || cache == null || resourceGroupMap == null
        || currentStateOutput == null || bestPossibleStateOutput == null)
    {
      throw new StageException("Missing attributes in event:" + event
       + ". Requires ClusterManager|DataCache|RESOURCE_GROUPS|CURRENT_STATE|BEST_POSSIBLE_STATE");
    }

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    Map<String, String> sessionIdMap = new HashMap<String, String>();

    for (LiveInstance liveInstance : liveInstances.values())
    {
      sessionIdMap.put(liveInstance.getInstanceName(),
          liveInstance.getSessionId());
    }
    MessageGenerationOutput output = new MessageGenerationOutput();

    for (String resourceGroupName : resourceGroupMap.keySet())
    {
      ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
      StateModelDefinition stateModelDef = cache.getStateModelDef(resourceGroup.getStateModelDefRef());
      for (ResourceKey resource : resourceGroup.getResourceKeys())
      {
        Map<String, String> instanceStateMap = bestPossibleStateOutput
            .getInstanceStateMap(resourceGroupName, resource);

        for (String instanceName : instanceStateMap.keySet())
        {
          String desiredState = instanceStateMap.get(instanceName);

          String currentState = currentStateOutput.getCurrentState(
              resourceGroupName, resource, instanceName);
          if (currentState == null)
          {
            currentState = stateModelDef.getInitialState();
          }

          String pendingState = currentStateOutput.getPendingState(
              resourceGroupName, resource, instanceName);

          String nextState;
            nextState = stateModelDef.getNextStateForTransition(currentState,
                desiredState);

          if (!desiredState.equalsIgnoreCase(currentState))
          {
            if (nextState != null)
            {
              if (pendingState != null
                  && nextState.equalsIgnoreCase(pendingState))
              {
                if (logger.isDebugEnabled())
                {
                  logger.debug("Message already exists at" + instanceName
                               + " to transition"+ resource.getResourceKeyName() +" from "
                               + currentState + " to " + nextState );
                }
              } else
              {
                Message message = createMessage(manager,resourceGroupName,
                    resource.getResourceKeyName(), instanceName, currentState,
                    nextState, sessionIdMap.get(instanceName), stateModelDef.getId());

                output.addMessage(resourceGroupName, resource, message);
              }
            } else
            {
              logger
                  .warn("Unable to find a next state from stateModelDefinition"
                      + stateModelDef.getClass() + " from:" + currentState
                      + " to:" + desiredState);
            }
          }
        }

      }
    }
    event.addAttribute(AttributeName.MESSAGES_ALL.toString(), output);
  }

  private Message createMessage(ClusterManager manager,String resourceGroupName,
      String resourceKeyName, String instanceName, String currentState,
      String nextState, String sessionId, String stateModelDefName)
  {
    String uuid = UUID.randomUUID().toString();
    Message message = new Message(MessageType.STATE_TRANSITION,uuid);
    message.setMsgId(uuid);
    String hostName = "UNKNOWN";
    try
    {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e)
    {
      logger.info("Unable to get Host name. Will set it to UNKNOWN, mostly ignorable", e);
      // can ignore it,
    }
    message.setSrcName(manager.getInstanceName() + "_" + hostName);
    message.setTgtName(instanceName);
    message.setMsgState("new");
    message.setStateUnitKey(resourceKeyName);
    message.setStateUnitGroup(resourceGroupName);
    message.setFromState(currentState);
    message.setToState(nextState);
    message.setTgtSessionId(sessionId);
    message.setSrcSessionId(manager.getSessionId());
    message.setStateModelDef(stateModelDefName);
    return message;
  }
}
