package com.linkedin.helix.controller.stages;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.ResourceGroup;

/**
 * This stage computes all the resources in a cluster. The resources are
 * computed from IdealStates -> this gives all the resources currently active
 * CurrentState for liveInstance-> Helps in finding resources that are inactive
 * and needs to be dropped
 * 
 * @author kgopalak
 * 
 */
public class ResourceComputationStage extends AbstractBaseStage
{
  private static Logger LOG = Logger.getLogger(ResourceComputationStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    if (cache == null)
    {
      throw new StageException("Missing attributes in event:" + event + ". Requires DataCache");
    }

    Map<String, IdealState> idealStates = cache.getIdealStates();

    Map<String, ResourceGroup> resourceGroupMap = new LinkedHashMap<String, ResourceGroup>();

    if (idealStates != null && idealStates.size() > 0)
    {
      for (IdealState idealState : idealStates.values())
      {
        Set<String> resourceSet = idealState.getResourceKeySet();
        String resourceGroupName = idealState.getResourceGroup();
        String factoryName = idealState.getStateModelFactoryName();

        for (String resourceKey : resourceSet)
        {
          addResource(resourceKey, resourceGroupName, factoryName, resourceGroupMap);
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

        Map<String, CurrentState> currentStateMap = cache.getCurrentState(instanceName,
            clientSessionId);
        if (currentStateMap == null || currentStateMap.size() == 0)
        {
          continue;
        }
        for (CurrentState currentState : currentStateMap.values())
        {
          String resourceGroupName = currentState.getResourceGroupName();
          Map<String, String> resourceStateMap = currentState.getResourceKeyStateMap();
          addResourceGroup(resourceGroupName, resourceGroupMap);

          for (String resourceKey : resourceStateMap.keySet())
          {
            addResource(resourceKey, resourceGroupName, null, resourceGroupMap);
            ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);

            if (currentState.getStateModelDefRef() == null)
            {
              LOG.error("state model def is null." + "resourceGroup:"
                  + currentState.getResourceGroupName() + ", resourceKeys: "
                  + currentState.getResourceKeyStateMap().keySet() + ", states: "
                  + currentState.getResourceKeyStateMap().values());
              throw new StageException("State model def is null for resourceGroup:"
                  + currentState.getResourceGroupName());
            }

            resourceGroup.setStateModelDefRef(currentState.getStateModelDefRef());
          }
        }
      }
    }
    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(), resourceGroupMap);
  }

  private void addResourceGroup(String resGroup, Map<String, ResourceGroup> resGroupMap)
  {
    if (resGroup == null || resGroupMap == null)
    {
      return;
    }

    if (!resGroupMap.containsKey(resGroup))
    {
      resGroupMap.put(resGroup, new ResourceGroup(resGroup));
    }
  }

  private void addResource(String resourceKey, String resourceGroupName, String factoryName,
      Map<String, ResourceGroup> resourceGroupMap)
  {
    if (resourceGroupName == null || resourceKey == null || resourceGroupMap == null)
    {
      return;
    }
    if (!resourceGroupMap.containsKey(resourceGroupName))
    {
      resourceGroupMap.put(resourceGroupName, new ResourceGroup(resourceGroupName, factoryName));
    }
    ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
    resourceGroup.addResource(resourceKey);

  }
}
