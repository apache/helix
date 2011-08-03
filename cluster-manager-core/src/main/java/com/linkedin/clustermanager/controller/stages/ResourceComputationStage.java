package com.linkedin.clustermanager.controller.stages;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.ResourceGroup;

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
      for (ZNRecord idealState : idealStates)
      {
        String resourceGroupName = idealState.getId();
        Map<String, Map<String, String>> resourceMappings = idealState
            .getMapFields();
        for (String resourceKey : resourceMappings.keySet())
        {
          addResource(resourceKey, resourceGroupName, resourceGroupMap);
        }
      }
    }
    // Its important to get resourceKeys from CurrentState as well since the
    // idealState might be removed. 
    List<ZNRecord> liveInstances = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.LIVEINSTANCES);
    if (liveInstances != null && liveInstances.size() > 0)
    {
      for (ZNRecord liveInstance : liveInstances)
      {
        String instanceName = liveInstance.getId();
        List<ZNRecord> currentStates = dataAccessor.getInstancePropertyList(
            instanceName, InstancePropertyType.CURRENTSTATES);
        if (currentStates == null || currentStates.size() == 0)
        {
          continue;
        }
        for (ZNRecord currentState : currentStates)
        {
          String resourceGroupName = currentState.getId();
          Map<String, Map<String, String>> mapFields = currentState
              .getMapFields();
          for (String resourceKey : mapFields.keySet())
          {
            addResource(resourceKey, resourceGroupName, resourceGroupMap);
          }
        }
      }
    }
    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(), resourceGroupMap);
  }

  private void addResource(String resourceKey, String resourceGroupName,
      Map<String, ResourceGroup> resourceGroupMap)
  {
    if (!resourceGroupMap.containsKey(resourceGroupName))
    {
      resourceGroupMap.put(resourceGroupName, new ResourceGroup(
          resourceGroupName));
    }
    ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
    resourceGroup.addResource(resourceKey);

  }
}
