package com.linkedin.clustermanager.controller.stages;

import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

public class CurrentStateComputationStage extends AbstractBaseStage
{
  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    if (manager == null)
    {
      throw new StageException("clustermanager attribute value is null");
    }
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    List<ZNRecord> liveInstances;
    liveInstances = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.LIVEINSTANCES);
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());

    for (ZNRecord record : liveInstances)
    {
      LiveInstance instance = new LiveInstance(record);
      String instanceName = record.getId();
      List<ZNRecord> instancePropertyList;
      instancePropertyList = dataAccessor.getInstancePropertyList(instanceName,
          InstancePropertyType.MESSAGES);
      for (ZNRecord messageRecord : instancePropertyList)
      {
        Message message = new Message(messageRecord);

        if (!instance.getSessionId().equals(message.getTgtSessionId()))
        {
          continue;
        }
        String resourceGroupName = message.getResourceGroupName();
        ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
        if (resourceGroup == null)
        {
          continue;
        }
        ResourceKey resourceKey = resourceGroup.getResourceKey(message
            .getResourceKey());
        if (resourceKey != null)
        {
          currentStateOutput.setPendingState(resourceGroupName, resourceKey,
              instanceName, message.getToState());
        } else
        {
          // log
        }
      }
    }
    for (ZNRecord record : liveInstances)
    {
      LiveInstance instance = new LiveInstance(record);
      String instanceName = record.getId();
      List<ZNRecord> instancePropertyList;
      String clientSessionId = record
          .getSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString());
      instancePropertyList = dataAccessor.getInstancePropertyList(instanceName,
          clientSessionId, InstancePropertyType.CURRENTSTATES);
      for (ZNRecord currStateRecord : instancePropertyList)
      {
        CurrentState currentState = new CurrentState(currStateRecord);

        if (!instance.getSessionId().equals(currentState.getSessionId()))
        {
          continue;
        }
        String resourceGroupName = currentState.getResourceGroupName();
        String stateModelDefName = currentState.getStateModelDefRef();
        ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
        if (resourceGroup == null)
        {
          continue;
        }
        if(stateModelDefName != null)
        {
          currentStateOutput.setResourceGroupStateModelDef(resourceGroupName, stateModelDefName);
        }
        Map<String, String> resourceKeyStateMap = currentState
            .getResourceKeyStateMap();
        for (String resourceKeyStr : resourceKeyStateMap.keySet())
        {
          ResourceKey resourceKey = resourceGroup
              .getResourceKey(resourceKeyStr);
          if (resourceKey != null)
          {
            currentStateOutput.setCurrentState(resourceGroupName, resourceKey,
                instanceName, currentState.getState(resourceKeyStr));
            
          } else
          {
            // log
          }
        }
      }

    }
    event.addAttribute(AttributeName.CURRENT_STATE.toString(),
        currentStateOutput);
  }
}
