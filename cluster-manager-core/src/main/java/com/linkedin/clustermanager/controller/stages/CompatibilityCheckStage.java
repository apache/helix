package com.linkedin.clustermanager.controller.stages;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

public class CompatibilityCheckStage extends AbstractBaseStage
{
  private static final Logger LOG = Logger
      .getLogger(CompatibilityCheckStage.class.getName());

  private boolean isCompatible(String controllerVersion, String participantVersion)
  {
    // TODO add compatible check logic here
    return controllerVersion.compareTo(participantVersion) >= 0;
  }

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    if (manager == null || cache == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires ClusterManager | DataCache");
    }

    String version = manager.getVersion();
    if (version == null)
    {
      // ignore version check
      LOG.warn("no version information set in controller (cluster manager version < 0.4.0):"
             + manager.getInstanceName()
             + ", ignore version check");
      return;
    }

    Map<String, LiveInstance> liveInstanceMap = cache.getLiveInstances();
    for (LiveInstance liveInstance : liveInstanceMap.values())
    {
      String participantVersion = liveInstance.getVersion();
      if (participantVersion == null)
      {
        // ignore version check
        LOG.warn("no version information set in participant (cluster manager version < 0.4.0):"
               + liveInstance.getInstanceName()
               + ", ignore version check");
      }
      else if (!isCompatible(version, participantVersion))
      {
        String errorMsg = "cluster manager versions are incompatible; pipeline will not continue. "
                        + "controllerVersion:" + version + ", participantVersion:" + participantVersion;
        LOG.error(errorMsg);
        throw new StageException(errorMsg);
      }
    }
  }
}
