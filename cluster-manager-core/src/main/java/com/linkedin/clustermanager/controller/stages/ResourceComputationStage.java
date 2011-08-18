package com.linkedin.clustermanager.controller.stages;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.Message.Attributes;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

public class ResourceComputationStage extends AbstractBaseStage
{

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    if (manager == null)
    {
      throw new StageException("ClusterManager attribute value is null");
    }
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();

    // GET resource list from IdealState.
    List<ZNRecord> idealStates = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.IDEALSTATES);
    Map<String, ResourceGroup> resourceGroupMap = new LinkedHashMap<String, ResourceGroup>();
    if (idealStates != null && idealStates.size() > 0)
    {
      for (ZNRecord idealStateRec : idealStates)
      {
        IdealState idealState = new IdealState(idealStateRec);
        String resourceGroupName = idealStateRec.getId();
        Map<String, Map<String, String>> resourceMappings = idealStateRec
            .getMapFields();
        for (String resourceKey : resourceMappings.keySet())
        {
          addResource(resourceKey, resourceGroupName, resourceGroupMap);
          ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
          resourceGroup.setStateModelDefRef(idealState.getStateModelDefRef());
        }
      }
    }
    // Its important to get resourceKeys from CurrentState as well since the
    // idealState might be removed.
    List<ZNRecord> availableInstances = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.LIVEINSTANCES);
    if (availableInstances != null && availableInstances.size() > 0)
    {
      for (ZNRecord instance : availableInstances)
      {
        String instanceName = instance.getId();
        String clientSessionId = instance.getSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString());
        List<ZNRecord> currentStates = dataAccessor.getInstancePropertyList(
            instanceName, clientSessionId, InstancePropertyType.CURRENTSTATES);
        if (currentStates == null || currentStates.size() == 0)
        {
          continue;
        }
        for (ZNRecord currentState : currentStates)
        {
          String resourceGroupName = currentState.getId();
          boolean idealStateExists = false;
          if (idealStates != null && idealStates.size() > 0)
          {
            for (ZNRecord idealStateRec : idealStates)
            {
              if(currentState.getId().equalsIgnoreCase(idealStateRec.getId()))
              {
                idealStateExists = true;
              }
            }
          }
          Map<String, Map<String, String>> mapFields = currentState
              .getMapFields();
          for (String resourceKey : mapFields.keySet())
          {
            // Skip dropped resources
            if(!idealStateExists)
            {
              if(mapFields.get(resourceKey).containsKey(AttributeName.LOCAL_STATE.toString()))
              {
                if(mapFields.get(resourceKey).get(AttributeName.LOCAL_STATE.toString()).equalsIgnoreCase("DROPPED"))
                {
                  continue;
                }
              }
            }
            addResource(resourceKey, resourceGroupName, resourceGroupMap);
            ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
            resourceGroup.setStateModelDefRef(currentState.getSimpleField(Message.Attributes.STATE_MODEL_DEF.toString()));
          }
        }
      }
    }
    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(),
        resourceGroupMap);
  }

  private void addResource(String resourceKey, String resourceGroupName,
      Map<String, ResourceGroup> resourceGroupMap)
  {
    if (resourceGroupName == null || resourceKey == null
        || resourceGroupMap == null)
    {
      return;
    }
    if (!resourceGroupMap.containsKey(resourceGroupName))
    {
      resourceGroupMap.put(resourceGroupName, new ResourceGroup(
          resourceGroupName));
    }
    ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
    resourceGroup.addResource(resourceKey);

  }
}
