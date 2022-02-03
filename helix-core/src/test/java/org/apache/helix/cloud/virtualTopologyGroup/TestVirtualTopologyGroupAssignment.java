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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.cloud.constants.VirtualTopologyGroupConstants;
import org.apache.helix.cloud.topology.FifoVirtualGroupAssignmentAlgorithm;
import org.apache.helix.cloud.topology.VirtualGroupAssignmentAlgorithm;
import org.apache.helix.util.HelixUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestVirtualTopologyGroupAssignment {

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

  @DataProvider
  public Object[][] getMappingTests() {
    Map<String, Set<String>> virtualMapping = new HashMap<>();
    VirtualGroupAssignmentAlgorithm algorithm = FifoVirtualGroupAssignmentAlgorithm.getInstance();
    virtualMapping.put(computeVirtualGroupId(0), Sets.newHashSet("1", "2", "3", "4", "5"));
    virtualMapping.put(computeVirtualGroupId(1), Sets.newHashSet("6", "7", "8", "9"));
    virtualMapping.put(computeVirtualGroupId(2), Sets.newHashSet("a", "b", "c", "d"));
    Assert.assertEquals(algorithm.computeAssignment(3, GROUP_NAME, _zoneMapping),
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
    return new Object[][] {
        {3, virtualMapping, algorithm},
        {8, virtualMapping2, algorithm}
    };
  }

  private static String computeVirtualGroupId(int groupIndex) {
    return GROUP_NAME + VirtualTopologyGroupConstants.GROUP_NAME_SPLITTER + groupIndex;
  }
}
