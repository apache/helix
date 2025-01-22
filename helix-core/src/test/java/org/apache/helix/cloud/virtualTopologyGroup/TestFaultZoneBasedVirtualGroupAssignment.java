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
  private Map<String, Set<String>> _zoneMapping2 = new HashMap<>();

  @BeforeTest
  public void prepare() {
    _zoneMapping = new HashMap<>();
    _zoneMapping2 = new HashMap<>();
    int instanceIdx = 0;
    for (int i = 0; i < ZONE_NUMBER; i++) {
      String zone = "zone_" + i;
      _zoneMapping.computeIfAbsent(zone, k -> new HashSet<>());
      _zoneMapping2.computeIfAbsent(zone, k -> new HashSet<>());
      for (int j = 0; j < INSTANCES_PER_ZONE; j++) {
        String instance = "instance_" + instanceIdx++;
        _zoneMapping.get(zone).add(instance);
        _zoneMapping2.get(zone).add(instance);
      }
    }
    // Add a branch zone zone_20 to zoneMapping2
    _zoneMapping2.computeIfAbsent("zone_20", k -> new HashSet<>());
    for (int j = 0; j < INSTANCES_PER_ZONE; j++) {
      String instance = "instance_" + instanceIdx++;
      _zoneMapping2.get("zone_" + (ZONE_NUMBER)).add(instance);
    }
  }

  @Test(dataProvider = "getMappingTests")
  public void testAssignmentScheme(int numGroups, Map<String, Set<String>> expected,
      VirtualGroupAssignmentAlgorithm algorithm, Map<String, Set<String>> zoneMapping,
      Map<String, Set<String>> virtualMapping) {
    Assert.assertEquals(
        algorithm.computeAssignment(numGroups, GROUP_NAME, zoneMapping, virtualMapping), expected);
  }

  @DataProvider
  public Object[][] getMappingTests() {
    VirtualGroupAssignmentAlgorithm algorithm = FaultZoneBasedVirtualGroupAssignmentAlgorithm.getInstance();

    // The virtual groups should be balanced across zones
    Map<String, Set<String>> virtualMapping = new HashMap<>();

    virtualMapping.put(computeVirtualGroupId(0, GROUP_NAME), new HashSet<>());
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_5"));
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_1"));
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_16"));
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_7"));
    virtualMapping.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_14"));

    virtualMapping.put(computeVirtualGroupId(1, GROUP_NAME), new HashSet<>());
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_0"));
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_12"));
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_3"));
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_18"));
    virtualMapping.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_10"));

    virtualMapping.put(computeVirtualGroupId(2, GROUP_NAME), new HashSet<>());
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_17"));
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_9"));
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_11"));
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_19"));
    virtualMapping.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_4"));

    virtualMapping.put(computeVirtualGroupId(3, GROUP_NAME), new HashSet<>());
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_13"));
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_6"));
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_2"));
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_15"));
    virtualMapping.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_8"));


    Map<String, Set<String>> virtualMapping2 = new HashMap<>();
    virtualMapping2.put(computeVirtualGroupId(0, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_1"));
    virtualMapping2.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping.get("zone_12"));

    virtualMapping2.put(computeVirtualGroupId(1, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_0"));
    virtualMapping2.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_5"));
    virtualMapping2.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping.get("zone_13"));

    virtualMapping2.put(computeVirtualGroupId(2, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_17"));
    virtualMapping2.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_6"));
    virtualMapping2.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping.get("zone_15"));

    virtualMapping2.put(computeVirtualGroupId(3, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_19"));
    virtualMapping2.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_8"));
    virtualMapping2.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping.get("zone_9"));

    virtualMapping2.put(computeVirtualGroupId(4, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(4, GROUP_NAME)).addAll(_zoneMapping.get("zone_7"));
    virtualMapping2.get(computeVirtualGroupId(4, GROUP_NAME)).addAll(_zoneMapping.get("zone_10"));
    virtualMapping2.get(computeVirtualGroupId(4, GROUP_NAME)).addAll(_zoneMapping.get("zone_18"));

    virtualMapping2.put(computeVirtualGroupId(5, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(5, GROUP_NAME)).addAll(_zoneMapping.get("zone_3"));
    virtualMapping2.get(computeVirtualGroupId(5, GROUP_NAME)).addAll(_zoneMapping.get("zone_16"));
    virtualMapping2.get(computeVirtualGroupId(5, GROUP_NAME)).addAll(_zoneMapping.get("zone_14"));

    virtualMapping2.put(computeVirtualGroupId(6, GROUP_NAME), new HashSet<>());
    virtualMapping2.get(computeVirtualGroupId(6, GROUP_NAME)).addAll(_zoneMapping.get("zone_11"));
    virtualMapping2.get(computeVirtualGroupId(6, GROUP_NAME)).addAll(_zoneMapping.get("zone_2"));
    virtualMapping2.get(computeVirtualGroupId(6, GROUP_NAME)).addAll(_zoneMapping.get("zone_4"));


    Map<String, Set<String>> virtualMapping3 = new HashMap<>();
    virtualMapping3.put(computeVirtualGroupId(0, GROUP_NAME), new HashSet<>());
    virtualMapping3.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping2.get("zone_1"));
    virtualMapping3.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping2.get("zone_12"));
    virtualMapping3.get(computeVirtualGroupId(0, GROUP_NAME)).addAll(_zoneMapping2.get("zone_20"));

    virtualMapping3.put(computeVirtualGroupId(1, GROUP_NAME), new HashSet<>());
    virtualMapping3.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping2.get("zone_0"));
    virtualMapping3.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping2.get("zone_5"));
    virtualMapping3.get(computeVirtualGroupId(1, GROUP_NAME)).addAll(_zoneMapping2.get("zone_13"));

    virtualMapping3.put(computeVirtualGroupId(2, GROUP_NAME), new HashSet<>());
    virtualMapping3.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping2.get("zone_17"));
    virtualMapping3.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping2.get("zone_6"));
    virtualMapping3.get(computeVirtualGroupId(2, GROUP_NAME)).addAll(_zoneMapping2.get("zone_15"));

    virtualMapping3.put(computeVirtualGroupId(3, GROUP_NAME), new HashSet<>());
    virtualMapping3.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping2.get("zone_19"));
    virtualMapping3.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping2.get("zone_8"));
    virtualMapping3.get(computeVirtualGroupId(3, GROUP_NAME)).addAll(_zoneMapping2.get("zone_9"));

    virtualMapping3.put(computeVirtualGroupId(4, GROUP_NAME), new HashSet<>());
    virtualMapping3.get(computeVirtualGroupId(4, GROUP_NAME)).addAll(_zoneMapping2.get("zone_7"));
    virtualMapping3.get(computeVirtualGroupId(4, GROUP_NAME)).addAll(_zoneMapping2.get("zone_10"));
    virtualMapping3.get(computeVirtualGroupId(4, GROUP_NAME)).addAll(_zoneMapping2.get("zone_18"));

    virtualMapping3.put(computeVirtualGroupId(5, GROUP_NAME), new HashSet<>());
    virtualMapping3.get(computeVirtualGroupId(5, GROUP_NAME)).addAll(_zoneMapping2.get("zone_3"));
    virtualMapping3.get(computeVirtualGroupId(5, GROUP_NAME)).addAll(_zoneMapping2.get("zone_16"));
    virtualMapping3.get(computeVirtualGroupId(5, GROUP_NAME)).addAll(_zoneMapping2.get("zone_14"));

    virtualMapping3.put(computeVirtualGroupId(6, GROUP_NAME), new HashSet<>());
    virtualMapping3.get(computeVirtualGroupId(6, GROUP_NAME)).addAll(_zoneMapping2.get("zone_11"));
    virtualMapping3.get(computeVirtualGroupId(6, GROUP_NAME)).addAll(_zoneMapping2.get("zone_2"));
    virtualMapping3.get(computeVirtualGroupId(6, GROUP_NAME)).addAll(_zoneMapping2.get("zone_4"));

    return new Object[][]{{4, virtualMapping, algorithm, _zoneMapping, new HashMap<>()},
        {7, virtualMapping2, algorithm, _zoneMapping, new HashMap<>()},
        // Should incrementally add the new zone to the virtual groups
        {7, virtualMapping3, algorithm, _zoneMapping2, virtualMapping2}};
  }
}
