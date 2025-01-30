package org.apache.helix.util;

import io.netty.util.internal.StringUtil;
import org.apache.helix.cloud.constants.VirtualTopologyGroupConstants;

public class VirtualTopologyUtil {
  public static String computeVirtualGroupId(int groupIndex, String virtualGroupName) {
    return virtualGroupName + VirtualTopologyGroupConstants.GROUP_NAME_SPLITTER + groupIndex;
  }

  /**
   * Ensures the provided fault zone type string ends with
   * the virtual fault zone type suffix.
   *
   * @param oldFaultZoneType The original fault zone type. Must not be null or empty.
   * @return The fault zone type string with the virtual fault zone type appended if necessary.
   * @throws IllegalArgumentException if {@code oldFaultZoneType} is null or empty
   */
  public static String computeVirtualFaultZoneTypeKey(String oldFaultZoneType) {
    if (StringUtil.isNullOrEmpty(oldFaultZoneType)) {
      throw new IllegalArgumentException("The old fault zone type is null or empty");
    }

    String suffix = VirtualTopologyGroupConstants.GROUP_NAME_SPLITTER
        + VirtualTopologyGroupConstants.VIRTUAL_FAULT_ZONE_TYPE;

    // If already ends with splitter + VIRTUAL_FAULT_ZONE_TYPE, return as-is
    if (oldFaultZoneType.endsWith(suffix)) {
      return oldFaultZoneType;
    }

    // Otherwise, remove any existing suffix parts beyond the first splitter, if needed
    String[] segments = oldFaultZoneType.split(VirtualTopologyGroupConstants.GROUP_NAME_SPLITTER);
    String baseName = segments[0];

    return baseName + suffix;
  }
}
