package com.linkedin.clustermanager.controller.stages;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

/**
 * This stage computes all the resources in a cluster. The resources are computed from
 * IdealStates -> this gives all the resources currently active CurrentState for
 * liveInstance-> Helps in finding resources that are inactive and needs to be dropped
 *
 * @author kgopalak
 *
 */
public class ResourceComputationStage extends AbstractBaseStage
{
  private static Logger logger = Logger.getLogger(ResourceComputationStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    if (cache == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires DataCache");
    }

    Map<String, IdealState> idealStates = cache.getIdealStates();

    Map<String, ResourceGroup> resourceGroupMap =
        new LinkedHashMap<String, ResourceGroup>();

    if (idealStates != null && idealStates.size() > 0)
    {
      for (IdealState idealState : idealStates.values())
      {
        Set<String> resourceSet = idealState.getResourceKeySet();
        String resourceGroupName = idealState.getResourceGroup();

        for (String resourceKey : resourceSet)
        {
          addResource(resourceKey, resourceGroupName, resourceGroupMap);
          ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
          resourceGroup.setStateModelDefRef(idealState.getStateModelDefRef());
        }

      }
    }

    // It's important to get resourceKeys from CurrentState as well since the
    // idealState might be removed.
    Map<String, LiveInstance> availableInstances = cache.getLiveInstances();

    if (availableInstances != null && availableInstances.size() > 0)
    {
      for (LiveInstance instance : availableInstances.values())
      {
        String instanceName = instance.getInstanceName();
        String clientSessionId = instance.getSessionId();

        Map<String, CurrentState> currentStateMap =
            cache.getCurrentState(instanceName, clientSessionId);
        if (currentStateMap == null || currentStateMap.size() == 0)
        {
          continue;
        }
        for (CurrentState currentState : currentStateMap.values())
        {
          String resourceGroupName = currentState.getResourceGroupName();
          Map<String, String> resourceStateMap = currentState.getResourceKeyStateMap();

          for (String resourceKey : resourceStateMap.keySet())
          {
            addResource(resourceKey, resourceGroupName, resourceGroupMap);
            ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
            resourceGroup.setStateModelDefRef(currentState.getStateModelDefRef());
          }
        }
      }
    }
    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(), resourceGroupMap);
  }

  private void addResource(String resourceKey,
                           String resourceGroupName,
                           Map<String, ResourceGroup> resourceGroupMap)
  {
    if (resourceGroupName == null || resourceKey == null || resourceGroupMap == null)
    {
      return;
    }
    if (!resourceGroupMap.containsKey(resourceGroupName))
    {
      resourceGroupMap.put(resourceGroupName, new ResourceGroup(resourceGroupName));
    }
    ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
    resourceGroup.addResource(resourceKey);

  }
}
