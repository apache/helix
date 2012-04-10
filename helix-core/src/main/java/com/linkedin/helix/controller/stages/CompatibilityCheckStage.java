package com.linkedin.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.LiveInstance;

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
       * {controllerPrimaryVersion,participantPrimaryVersion} -> false
       */
      map.put("0.4,0.3", false);
      INCOMPATIBLE_MAP = Collections.unmodifiableMap(map);
  }

  private String getPrimaryVersion(String version)
  {
    String[] splits = version.split("\\.");
    if (splits == null || splits.length != 3)
    {
      return null;
    }
    return version.substring(0, version.lastIndexOf('.'));
  }

  private boolean isCompatible(String controllerVersion, String participantVersion)
  {
    if (participantVersion == null)
    {
      LOG.warn("Missing version of participant. Skip version check.");
      return true;
    }

    // compare primary version
    String controllerPrimaryVersion = getPrimaryVersion(controllerVersion);
    String participantPrimaryVersion = getPrimaryVersion(participantVersion);
    if (controllerPrimaryVersion != null && participantPrimaryVersion != null)
    {
      if (controllerPrimaryVersion.compareTo(participantPrimaryVersion) < 0)
      {
        LOG.info("Controller primary version is less than participant primary version.");
        return false;
      }
      else
      {
        if (INCOMPATIBLE_MAP.containsKey(controllerPrimaryVersion + "," + participantPrimaryVersion))
        {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    HelixManager manager = event.getAttribute("helixmanager");
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    if (manager == null || cache == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager | DataCache");
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
      String participantVersion = liveInstance.getHelixVersion();
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
