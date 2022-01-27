package org.apache.helix.cloud.virtualTopologyGroup;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.cloud.constants.VirtualTopologyGroupConstants;
import org.apache.helix.cloud.topology.VirtualGroupAssignmentAlgorithm;
import org.apache.helix.cloud.topology.VirtualTopologyGroupScheme;
import org.apache.helix.util.HelixUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestVirtualTopologyGroup {

  private static final String GROUP_NAME = "test_virtual_group";
  private final List<String> _flattenExpected = Arrays.asList(
      "1", "2", "3",
      "4", "5", "6",
      "7", "8", "9",
      "a", "b", "c", "d");
  private Map<String, Set<String>> _zoneMapping = new HashMap<>();

  @BeforeTest
  public void prepare() {
    _zoneMapping = new HashMap<>();
    _zoneMapping.put("c", Sets.newHashSet("9", "8", "7"));
    _zoneMapping.put("a", Sets.newHashSet("2", "3", "1"));
    _zoneMapping.put("z", Sets.newHashSet("b", "c", "d", "a"));
    _zoneMapping.put("b", Sets.newHashSet("5", "4", "6"));
  }

  @Test
  public void testFlattenZoneMapping() {
    Assert.assertEquals(HelixUtil.sortAndFlattenZoneMapping(_zoneMapping), _flattenExpected);
  }

  @Test(dataProvider = "getMappingTests")
  public void testAssignmentScheme(int numGroups, Map<String, Set<String>> expected,
      VirtualGroupAssignmentAlgorithm algorithm) {
    Assert.assertEquals(algorithm.computeAssignment(numGroups, GROUP_NAME, _zoneMapping), expected);
  }

  @Test
  public void assignmentBenchmark() {
    Map<String, Set<String>> testMapping = createZoneMapping(new int[] {40, 40, 40, 40, 40}, "zone");
    singleAssignmentBenchmark(testMapping, 13);
    singleAssignmentBenchmark(testMapping, 20);

    int numGroups = 20;
    Map<String, Set<String>> testMapping2 = createZoneMapping(new int[] {40, 40, 40, 40, 40}, "zone");
    testMapping2.get("zone_2").add("2_201");
    testMapping2.get("zone_2").add("2_202");
    testMapping2.get("zone_3").add("3_301");
    testMapping2.get("zone_3").add("3_302");
    compareAssignmentUnderDifferentZoneMapping(testMapping, testMapping2, numGroups, VirtualTopologyGroupScheme.FIFO);
    compareAssignmentUnderDifferentZoneMapping(testMapping, testMapping2, numGroups, VirtualTopologyGroupScheme.FIFO_COMBINED_ROUND_ROBIN);
    compareAssignmentUnderDifferentZoneMapping(testMapping, testMapping2, numGroups, VirtualTopologyGroupScheme.ROUND_ROBIN);

    Map<String, Set<String>> testMapping3 = createZoneMapping(new int[] {40, 40, 40, 40, 40}, "zone");
    testMapping3.get("zone_2").remove("2_13");
    testMapping3.get("zone_2").remove("2_8");
    testMapping3.get("zone_4").remove("4_8");
    testMapping3.get("zone_4").remove("4_17");
    compareAssignmentUnderDifferentZoneMapping(testMapping, testMapping3, numGroups, VirtualTopologyGroupScheme.FIFO);
    compareAssignmentUnderDifferentZoneMapping(testMapping, testMapping3, numGroups, VirtualTopologyGroupScheme.FIFO_COMBINED_ROUND_ROBIN);
    compareAssignmentUnderDifferentZoneMapping(testMapping, testMapping3, numGroups, VirtualTopologyGroupScheme.ROUND_ROBIN);
  }

  private static void singleAssignmentBenchmark(Map<String, Set<String>> testMapping, int numGroups) {
    System.out.println("Test mapping with numGroups: " + numGroups);
    System.out.println(testMapping);
    System.out.println("==========");
    validate(numGroups, testMapping, VirtualTopologyGroupScheme.FIFO);
    validate(numGroups, testMapping, VirtualTopologyGroupScheme.FIFO_COMBINED_ROUND_ROBIN);
    validate(numGroups, testMapping, VirtualTopologyGroupScheme.ROUND_ROBIN);
  }

  private static void compareAssignmentUnderDifferentZoneMapping(
      Map<String, Set<String>> baseMapping,
      Map<String, Set<String>> testMapping,
      int numGroups,
      VirtualGroupAssignmentAlgorithm algorithm) {
    Map<String, Set<String>> baseAssignment =
        algorithm.computeAssignment(numGroups, TestVirtualTopologyGroup.GROUP_NAME, baseMapping);
    Map<String, Set<String>> testAssignment =
        algorithm.computeAssignment(numGroups, TestVirtualTopologyGroup.GROUP_NAME, testMapping);
    AssignmentEvaluation base = new AssignmentEvaluation(baseAssignment, baseMapping);
    AssignmentEvaluation test = new AssignmentEvaluation(testAssignment, testMapping);
    System.out.println("Diff for " + algorithm + " : " + base.compareAssignments(test));
  }

  @DataProvider
  public Object[][] getMappingTests() {
    Map<String, Set<String>> virtualMapping = new HashMap<>();
    virtualMapping.put(computeVirtualGroupId(0), Sets.newHashSet("1", "2", "3", "4", "5"));
    virtualMapping.put(computeVirtualGroupId(1), Sets.newHashSet("6", "7", "8", "9"));
    virtualMapping.put(computeVirtualGroupId(2), Sets.newHashSet("a", "b", "c", "d"));
    Assert.assertEquals(VirtualTopologyGroupScheme.FIFO.computeAssignment(3, GROUP_NAME, _zoneMapping),
        virtualMapping);
    Map<String, Set<String>> virtualMapping2 = new HashMap<>();
    virtualMapping2.put(computeVirtualGroupId(0), Sets.newHashSet("1", "2"));
    virtualMapping2.put(computeVirtualGroupId(1), Sets.newHashSet("3", "4"));
    virtualMapping2.put(computeVirtualGroupId(2), Sets.newHashSet("5", "6"));
    virtualMapping2.put(computeVirtualGroupId(3), Sets.newHashSet("7", "8"));
    virtualMapping2.put(computeVirtualGroupId(4), Sets.newHashSet("9", "a"));
    virtualMapping2.put(computeVirtualGroupId(5), Sets.newHashSet("b"));
    virtualMapping2.put(computeVirtualGroupId(6), Sets.newHashSet("c"));
    virtualMapping2.put(computeVirtualGroupId(7), Sets.newHashSet("d"));

    Map<String, Set<String>> virtualMappingCombined = new HashMap<>();
    virtualMappingCombined.put(computeVirtualGroupId(0), Sets.newHashSet("1", "2", "3", "4"));
    virtualMappingCombined.put(computeVirtualGroupId(1), Sets.newHashSet("5", "6", "7", "8"));
    virtualMappingCombined.put(computeVirtualGroupId(2), Sets.newHashSet("9", "a", "b", "c", "d"));

    Map<String, Set<String>> virtualMappingCombined2 = new HashMap<>();
    virtualMappingCombined2.put(computeVirtualGroupId(0), Sets.newHashSet("1", "9"));
    virtualMappingCombined2.put(computeVirtualGroupId(1), Sets.newHashSet("2", "a"));
    virtualMappingCombined2.put(computeVirtualGroupId(2), Sets.newHashSet("3", "b"));
    virtualMappingCombined2.put(computeVirtualGroupId(3), Sets.newHashSet("4", "c"));
    virtualMappingCombined2.put(computeVirtualGroupId(4), Sets.newHashSet("5", "d"));
    virtualMappingCombined2.put(computeVirtualGroupId(5), Sets.newHashSet("6"));
    virtualMappingCombined2.put(computeVirtualGroupId(6), Sets.newHashSet("7"));
    virtualMappingCombined2.put(computeVirtualGroupId(7), Sets.newHashSet("8"));

    Map<String, Set<String>> virtualMappingRoundRobin = new HashMap<>();
    virtualMappingRoundRobin.put(computeVirtualGroupId(0), Sets.newHashSet("1", "4", "7", "a", "d"));
    virtualMappingRoundRobin.put(computeVirtualGroupId(1), Sets.newHashSet("2", "5", "8", "b"));
    virtualMappingRoundRobin.put(computeVirtualGroupId(2), Sets.newHashSet("3", "6", "9", "c"));

    return new Object[][] {
        {3, virtualMapping, VirtualTopologyGroupScheme.FIFO},
        {8, virtualMapping2, VirtualTopologyGroupScheme.FIFO},
        {3, virtualMappingCombined, VirtualTopologyGroupScheme.FIFO_COMBINED_ROUND_ROBIN},
        {8, virtualMappingCombined2, VirtualTopologyGroupScheme.FIFO_COMBINED_ROUND_ROBIN},
        {3, virtualMappingRoundRobin, VirtualTopologyGroupScheme.ROUND_ROBIN}
    };
  }

  private static String computeVirtualGroupId(int groupIndex) {
    return GROUP_NAME + VirtualTopologyGroupConstants.GROUP_NAME_SPLITTER + groupIndex;
  }

  private static void validate(int numGroups, Map<String, Set<String>> zoneMapping,
      VirtualGroupAssignmentAlgorithm algorithm) {
    Map<String, Set<String>> assignment =
        algorithm.computeAssignment(numGroups, TestVirtualTopologyGroup.GROUP_NAME, zoneMapping);
    System.out.println("Assignment using " + algorithm);
    System.out.println(assignment);
    AssignmentEvaluation evaluation = new AssignmentEvaluation(assignment, zoneMapping);
    evaluation.print();
    System.out.println("==========");
  }

  private static Map<String, Set<String>> createZoneMapping(int[] zoneInstances, String namePrefix) {
    Map<String, Set<String>> zoneMapping = new HashMap<>();
    int zoneInd = 0;
    for (int num : zoneInstances) {
      Set<String> instances = new HashSet<>();
      zoneMapping.put(namePrefix + VirtualTopologyGroupConstants.GROUP_NAME_SPLITTER + zoneInd, instances);
      for (int i = 0; i < num; i++) {
        String id = zoneInd + VirtualTopologyGroupConstants.GROUP_NAME_SPLITTER + i;
        instances.add(id);
      }
      zoneInd++;
    }
    return zoneMapping;
  }
}
