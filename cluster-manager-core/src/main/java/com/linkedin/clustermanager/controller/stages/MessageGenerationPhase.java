package com.linkedin.clustermanager.controller.stages;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

public class MessageGenerationPhase extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(MessageGenerationPhase.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    if (manager == null)
    {
      throw new StageException("ClusterManager attribute value is null");
    }
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    List<ZNRecord> stateModelDefs = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.STATEMODELDEFS);
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());

    CurrentStateOutput currentStateOutput = event
        .getAttribute(AttributeName.CURRENT_STATE.toString());

    BestPossibleStateOutput bestPossibleStateOutput = event
        .getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    List<ZNRecord> liveInstances = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.LIVEINSTANCES);
    Map<String, String> sessionIdMap = new HashMap<String, String>();
    for (ZNRecord record : liveInstances)
    {
      LiveInstance liveInstance = new LiveInstance(record);
      sessionIdMap.put(liveInstance.getInstanceName(),
          liveInstance.getSessionId());
    }
    MessageGenerationOutput output = new MessageGenerationOutput();
    
    for (String resourceGroupName : resourceGroupMap.keySet())
    {
      ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
      StateModelDefinition stateModelDef = lookupStateModel(
          resourceGroup.getStateModelDefRef(), stateModelDefs);
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
                  logger.debug("Message already exists to transition from "
                      + currentState + " to " + nextState);
                }
              } else
              {
                Message message = createMessage(resourceGroupName,
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

  private Message createMessage(String resourceGroupName,
      String resourceKeyName, String instanceName, String currentState,
      String nextState, String sessionId, String stateModelDefName)
  {
    Message message = new Message();
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
    message.setStateUnitKey(resourceKeyName);
    message.setStateUnitGroup(resourceGroupName);
    message.setFromState(currentState);
    message.setToState(nextState);
    message.setTgtSessionId(sessionId);
    message.setSrcSessionId(sessionId);
    message.setStateModelDef(stateModelDefName);
    return message;
  }

  private StateModelDefinition lookupStateModel(String stateModelDefRef,
      List<ZNRecord> stateModelDefs)
  {
    for (ZNRecord record : stateModelDefs)
    {
      if (record.getId().equals(stateModelDefRef))
      {
        return new StateModelDefinition(record);
      }
    }
    return null;
  }
}
