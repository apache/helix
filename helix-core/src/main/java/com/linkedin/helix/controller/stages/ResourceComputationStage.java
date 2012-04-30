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
import com.linkedin.helix.model.Resource;

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

    Map<String, Resource> resourceMap = new LinkedHashMap<String, Resource>();

    if (idealStates != null && idealStates.size() > 0)
    {
      for (IdealState idealState : idealStates.values())
      {
        Set<String> partitionSet = idealState.getPartitionSet();
        String resourceName = idealState.getResourceName();

        for (String partition : partitionSet)
        {
          addPartition(partition, resourceName, resourceMap);
          Resource resource = resourceMap.get(resourceName);
          resource.setStateModelDefRef(idealState.getStateModelDefRef());
          resource.setStateModelFactoryName(idealState.getStateModelFactoryName());
        }
      }
    }

    // It's important to get partitions from CurrentState as well since the
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

          String resourceName = currentState.getResourceName();
          Map<String, String> resourceStateMap = currentState.getPartitionStateMap();
          addResource(resourceName, resourceMap);
          Resource resource = resourceMap.get(resourceName);
          resource.setStateModelDefRef(currentState.getStateModelDefRef());
          resource.setStateModelFactoryName(currentState.getStateModelFactoryName());

          for (String partition : resourceStateMap.keySet())
          {
            addPartition(partition, resourceName, resourceMap);

            if (currentState.getStateModelDefRef() == null)
            {
              LOG.error("state model def is null." + "resource:" + currentState.getResourceName()
                  + ", partitions: " + currentState.getPartitionStateMap().keySet() + ", states: "
                  + currentState.getPartitionStateMap().values());
              throw new StageException("State model def is null for resource:"
                  + currentState.getResourceName());
            }
          }
        }
      }
    }
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
  }

  private void addResource(String resource, Map<String, Resource> resourceMap)
  {
    if (resource == null || resourceMap == null)
    {
      return;
    }
    if (!resourceMap.containsKey(resource))
    {
      resourceMap.put(resource, new Resource(resource));
    }
  }

  private void addPartition(String partition, String resourceName, Map<String, Resource> resourceMap)
  {
    if (resourceName == null || partition == null || resourceMap == null)
    {
      return;
    }
    if (!resourceMap.containsKey(resourceName))
    {
      resourceMap.put(resourceName, new Resource(resourceName));
    }
    Resource resource = resourceMap.get(resourceName);
    resource.addPartition(partition);

  }
}
