package org.apache.helix.cloud.virtualTopologyGroup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.cloud.topology.FaultZoneBasedVirtualGroupAssignmentAlgorithm;
import org.apache.helix.cloud.topology.VirtualGroupAssignmentAlgorithm;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.helix.util.VirtualTopologyUtil.computeVirtualGroupId;

public class TestFaultZoneBasedVirtualGroupAssignment {

  private static final String GROUP_NAME = "test_virtual_group";
  private static final int ZONE_NUMBER = 20;
  private static final int INSTANCES_PER_ZONE = 5;
  private Map<String, Set<String>> _zoneMapping = new HashMap<>();

  @BeforeTest
  public void prepare() {
    _zoneMapping = new HashMap<>();
    int instanceIdx = 0;
    for (int i = 0; i < ZONE_NUMBER; i++) {
      String zone = "zone_" + i;
      _zoneMapping.computeIfAbsent(zone, k -> new HashSet<>());
      for (int j = 0; j < INSTANCES_PER_ZONE; j++) {
        String instance = "instance_" + instanceIdx++;
        _zoneMapping.get(zone).add(instance);
      }
    }
  }

  @Test(dataProvider = "getMappingTests")
  public void testAssignmentScheme(int numGroups, Map<String, Set<String>> expected,
      VirtualGroupAssignmentAlgorithm algorithm) {
    Assert.assertEquals(algorithm.computeAssignment(numGroups, GROUP_NAME, _zoneMapping), expected);
  }

  @DataProvider
  public Object[][] getMappingTests() {
    VirtualGroupAssignmentAlgorithm algorithm = FaultZoneBasedVirtualGroupAssignmentAlgorithm.getInstance();

    // Ordered zones:
    // zone_0, zone_1, zone_10, zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17,
    // zone_18, zone_19, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9

    // Expected mapping with 4 virtual groups:
    // 0 -> zone_0, zone_1, zone_10, zone_11, zone_12
    // 1 -> zone_13, zone_14, zone_15, zone_16, zone_17
    // 2 -> zone_18, zone_19, zone_2, zone_3, zone_4
    // 3 -> zone_5, zone_6, zone_7, zone_8, zone_9
    Map<String, Set<String>> virtualMapping = new HashMap<>();

    virtualMapping.put(computeVirtualGroupId(0, GROUP_NAME), new HashSet<>());
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_0"));
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_1"));
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_10"));
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_11"));
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_12"));

    virtualMapping.put(computeVirtualGroupId(1, GROUP_NAME), new HashSet<>());
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_13"));
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_14"));
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_15"));
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_16"));
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_17"));

    virtualMapping.put(computeVirtualGroupId(2, GROUP_NAME), new HashSet<>());
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_18"));
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_19"));
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_2"));
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_3"));
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_4"));

    virtualMapping.put(computeVirtualGroupId(3, GROUP_NAME), new HashSet<>());
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_5"));
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_6"));
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_7"));
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_8"));
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_9"));

    // Expected mapping with 7 virtual groups:
    // 0 -> zone_0, zone_1, zone_10
    // 1 -> zone_11, zone_12, zone_13
    // 2 -> zone_14, zone_15, zone_16
    // 3 -> zone_17, zone_18, zone_19
    // 4 -> zone_2, zone_3, zone_4
    // 5 -> zone_5, zone_6, zone_7
    // 6 -> zone_8, zone_9
    Map<String, Set<String>> virtualMapping2 = new HashMap<>();
    virtualMapping2.put(computeVirtualGroupId(0, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_0"));
    virtualMapping2.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_1"));
    virtualMapping2.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_10"));

    virtualMapping2.put(computeVirtualGroupId(1, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_11"));
    virtualMapping2.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_12"));
    virtualMapping2.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_13"));

    virtualMapping2.put(computeVirtualGroupId(2, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_14"));
    virtualMapping2.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_15"));
    virtualMapping2.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_16"));

    virtualMapping2.put(computeVirtualGroupId(3, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_17"));
    virtualMapping2.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_18"));
    virtualMapping2.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_19"));

    virtualMapping2.put(computeVirtualGroupId(4, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(4, GROUP_NAME)).addAll(_zoneMapping.get("zone_2"));
    virtualMapping2.get(computeVirtualGroupId(4, GROUP_NAME)).addAll(_zoneMapping.get("zone_3"));
    virtualMapping2.get(computeVirtualGroupId(4, GROUP_NAME)).addAll(_zoneMapping.get("zone_4"));

    virtualMapping2.put(computeVirtualGroupId(5, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(5, GROUP_NAME)).addAll(_zoneMapping.get("zone_5"));
    virtualMapping2.get(computeVirtualGroupId(5, GROUP_NAME)).addAll(_zoneMapping.get("zone_6"));
    virtualMapping2.get(computeVirtualGroupId(5, GROUP_NAME)).addAll(_zoneMapping.get("zone_7"));

    virtualMapping2.put(computeVirtualGroupId(6, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(6, GROUP_NAME)).addAll(_zoneMapping.get("zone_8"));
    virtualMapping2.get(computeVirtualGroupId(6, GROUP_NAME)).addAll(_zoneMapping.get("zone_9"));

    return new Object[][]{{4, virtualMapping, algorithm}, {7, virtualMapping2, algorithm}};
  }
}
