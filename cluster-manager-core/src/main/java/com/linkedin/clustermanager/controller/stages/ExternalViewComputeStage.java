package com.linkedin.clustermanager.controller.stages;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

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
//    Map<String, InstanceConfig> configMap = cache.getInstanceConfigMap();

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
          Set<String> disabledInstances
            = cache.getDisabledInstancesForResource(resource.toString());
          // when set external view, ignore all disabled nodes
          for (String instance : currentStateMap.keySet())
          {
//            boolean isDisabled = configMap.containsKey(instance)
//                && configMap.get(instance).getInstanceEnabled() == false;
            if (!disabledInstances.contains(instance))
            {
              view.setState(resource.getResourceKeyName(), instance, currentStateMap.get(instance));
            }
          }
          // view.setStateMap(resource.getResourceKeyName(), currentStateMap);
        }
      }
      dataAccessor.setProperty(PropertyType.EXTERNALVIEW,
          view.getRecord(), resourceGroupName);
    }
    log.info("END ExternalViewComputeStage.process()");
  }

}
