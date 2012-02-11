package com.linkedin.helix.controller.stages;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.ResourceGroup;
import com.linkedin.helix.model.ResourceKey;

public class ExternalViewComputeStage extends AbstractBaseStage
{
  private static Logger log = Logger.getLogger(ExternalViewComputeStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    log.info("START ExternalViewComputeStage.process()");

    ClusterManager manager = event.getAttribute("clustermanager");
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    if (manager == null || resourceGroupMap == null || cache == null)
    {
      throw new StageException("Missing attributes in event:" + event
           + ". Requires ClusterManager|RESOURCE_GROUPS|DataCache");
    }

    ClusterDataAccessor dataAccessor = manager.getDataAccessor();

    CurrentStateOutput currentStateOutput = event
        .getAttribute(AttributeName.CURRENT_STATE.toString());

    for (String resourceGroupName : resourceGroupMap.keySet())
    {
      ExternalView view = new ExternalView(resourceGroupName);
      ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
      for (ResourceKey resource : resourceGroup.getResourceKeys())
      {
        Map<String, String> currentStateMap = currentStateOutput
            .getCurrentStateMap(resourceGroupName, resource);
        if (currentStateMap != null && currentStateMap.size() > 0)
        {
          // Set<String> disabledInstances
          // = cache.getDisabledInstancesForResource(resource.toString());
          for (String instance : currentStateMap.keySet())
          {
            // if (!disabledInstances.contains(instance))
            // {
              view.setState(resource.getResourceKeyName(), instance, currentStateMap.get(instance));
            // }
          }
          // view.setStateMap(resource.getResourceKeyName(), currentStateMap);
        }
      }
      dataAccessor.setProperty(PropertyType.EXTERNALVIEW, view, resourceGroupName);
    }
    log.info("END ExternalViewComputeStage.process()");
  }

}
