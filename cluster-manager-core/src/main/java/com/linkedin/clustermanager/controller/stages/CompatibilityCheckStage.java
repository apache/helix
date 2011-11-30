package com.linkedin.clustermanager.controller.stages;

import java.util.Collections;
import java.util.HashMap;
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

  /**
   * INCOMPATIBLE_MAP stores primary version pairs:
   *  {controllerPrimaryVersion, participantPrimaryVersion}
   *  that are incompatible
   */
  private static final Map<String, Boolean> INCOMPATIBLE_MAP;
  static
  {
      Map<String, Boolean> map = new HashMap<String, Boolean>();
      /**
       * {controllerPrimaryVersion, participantPrimaryVersion} -> false
       */
      map.put("0.4,0.3", false);
      INCOMPATIBLE_MAP = Collections.unmodifiableMap(map);
  }

  private boolean isCompatible(String controllerVersion, String participantVersion)
  {
    if (participantVersion == null)
    {
      LOG.warn("Missing version of participant. Skip version check.");
      return true;
    }

    // get the primary version
    int idx = controllerVersion.indexOf('.', controllerVersion.indexOf('.') + 1);
    String ctrlPrimVersion = controllerVersion.substring(0, idx);
    idx = participantVersion.indexOf('.', participantVersion.indexOf('.') + 1);
    String partPrimVersion = participantVersion.substring(0, idx);
    if (ctrlPrimVersion.compareTo(partPrimVersion) < 0)
    {
      LOG.info("Controller primary version is less than participant primary version.");
      return false;
    }
    else
    {
      if (INCOMPATIBLE_MAP.containsKey(ctrlPrimVersion + "," + partPrimVersion))
      {
        return false;
      }
      return true;
    }
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

    String controllerVersion = manager.getVersion();
    if (controllerVersion == null)
    {
      String errorMsg = "Missing version of controller: " + manager.getInstanceName()
          + ". Pipeline will not continue.";
      LOG.error(errorMsg);
      throw new StageException(errorMsg);
    }

    Map<String, LiveInstance> liveInstanceMap = cache.getLiveInstances();
    for (LiveInstance liveInstance : liveInstanceMap.values())
    {
      String participantVersion = liveInstance.getVersion();
      if (!isCompatible(controllerVersion, participantVersion))
      {
        String errorMsg = "cluster manager versions are incompatible; pipeline will not continue. "
                        + "controller:" + manager.getInstanceName() + ", controllerVersion:" + controllerVersion
                        + "; participant:" + liveInstance.getInstanceName() + ", participantVersion:" + participantVersion;
        LOG.error(errorMsg);
        throw new StageException(errorMsg);
      }
    }
  }
}
