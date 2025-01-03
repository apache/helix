package org.apache.helix.cloud.topology;

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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.helix.util.VirtualTopologyUtil.computeVirtualGroupId;

/**
 * A strategy that densely assign virtual groups with input instance list, it doesn't move to the next one until
 * the current one is filled.
 * Given that instances.size = instancesPerGroup * numGroups + residuals,
 * we break [residuals] into the first few groups, as a result each virtual group will have
 * either [instancesPerGroup] or [instancesPerGroup + 1] instances.
 */
public class FaultZoneBasedVirtualGroupAssignmentAlgorithm implements VirtualGroupAssignmentAlgorithm {

  private static final FaultZoneBasedVirtualGroupAssignmentAlgorithm _instance = new FaultZoneBasedVirtualGroupAssignmentAlgorithm();

  private FaultZoneBasedVirtualGroupAssignmentAlgorithm() { }

  public static FaultZoneBasedVirtualGroupAssignmentAlgorithm getInstance() {
    return _instance;
  }

  @Override
  public Map<String, Set<String>> computeAssignment(int numGroups, String virtualGroupName,
      Map<String, Set<String>> zoneMapping) {
    TreeSet<String> sortdFaultZones = new TreeSet<>(zoneMapping.keySet());
    int faultZonesPerGroup = sortdFaultZones.size() / numGroups;
    int residuals = sortdFaultZones.size() % numGroups;

    Map<String, Set<String>> assignment = new TreeMap<>();
    int virtualGroupIndex = 0;
    int curFaultZoneNum = 0;

    for (String faultZone : sortdFaultZones) {
      String groupId = computeVirtualGroupId(virtualGroupIndex, virtualGroupName);
      if (curFaultZoneNum == 0) {
        assignment.put(groupId, new HashSet<>());
      }
      assignment.get(groupId).addAll(zoneMapping.get(faultZone));
      curFaultZoneNum++;
      if (curFaultZoneNum == faultZonesPerGroup + (virtualGroupIndex < residuals ? 1 : 0)) {
        virtualGroupIndex++;
        curFaultZoneNum = 0;
      }
    }

    return assignment;
  }
}
