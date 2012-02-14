package com.linkedin.helix.controller.stages;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.Partition;

public class ExternalViewComputeStage extends AbstractBaseStage
{
  private static Logger log = Logger.getLogger(ExternalViewComputeStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    log.info("START ExternalViewComputeStage.process()");

    HelixManager manager = event.getAttribute("helixmanager");
    Map<String, Resource> resourceMap = event
        .getAttribute(AttributeName.RESOURCES.toString());
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    if (manager == null || resourceMap == null || cache == null)
    {
      throw new StageException("Missing attributes in event:" + event
           + ". Requires ClusterManager|RESOURCES|DataCache");
    }

    DataAccessor dataAccessor = manager.getDataAccessor();

    CurrentStateOutput currentStateOutput = event
        .getAttribute(AttributeName.CURRENT_STATE.toString());

    for (String resourceName : resourceMap.keySet())
    {
      ExternalView view = new ExternalView(resourceName);
      Resource resource = resourceMap.get(resourceName);
      for (Partition partition : resource.getPartitions())
      {
        Map<String, String> currentStateMap = currentStateOutput
            .getCurrentStateMap(resourceName, partition);
        if (currentStateMap != null && currentStateMap.size() > 0)
        {
          // Set<String> disabledInstances
          // = cache.getDisabledInstancesForResource(resource.toString());
          for (String instance : currentStateMap.keySet())
          {
            // if (!disabledInstances.contains(instance))
            // {
              view.setState(partition.getPartitionName(), instance, currentStateMap.get(instance));
            // }
          }
        }
      }
      dataAccessor.setProperty(PropertyType.EXTERNALVIEW, view, resourceName);
    }
    log.info("END ExternalViewComputeStage.process()");
  }

}
