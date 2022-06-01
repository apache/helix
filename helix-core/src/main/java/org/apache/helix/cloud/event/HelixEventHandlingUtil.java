package org.apache.helix.cloud.event;

import org.apache.helix.BaseDataAccessor;


public class HelixEventHandlingUtil {

  public static boolean enableInstanceForCloudEvent(String clusterName, String instanceName, boolean isEnable, long timeStamp,
      BaseDataAccessor dataAccessor) {
    return true;
  }

  public static boolean IsInstanceDisabledForCloudEvent(String clusterName, String instanceName,
      BaseDataAccessor dataAccessor) {
    return true;
  }

  public static Long getInstanceDisabledTimestamp(String clusterName, String instanceName,
      BaseDataAccessor dataAccessor) {
    return Long.parseLong("0");
  }

}
