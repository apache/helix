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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.cloud.constants.VirtualTopologyGroupConstants;
import org.apache.helix.util.HelixUtil;

public enum VirtualTopologyGroupScheme implements VirtualGroupAssignmentAlgorithm {

  /**
   * A strategy that densely assign virtual groups with input instance list, it doesn't move to the next one until
   * the current one is filled.
   * Given that instances.size = instancesPerGroup * numGroups + residuals,
   * we break [residuals] into the first few groups, as a result each virtual group will have
   * either [instancesPerGroup] or [instancesPerGroup + 1] instances.
   */
  FIFO {
    @Override
    public Map<String, Set<String>> computeAssignment(int numGroups, String virtualGroupName,
        Map<String, Set<String>> zoneMapping) {
      List<String> sortedInstances = HelixUtil.sortAndFlattenZoneMapping(zoneMapping);
      Map<String, Set<String>> assignment = new HashMap<>();
      // #instances = instancesPerGroupBase * numGroups + residuals
      int instancesPerGroupBase = sortedInstances.size() / numGroups;
      int residuals = sortedInstances.size() % numGroups; // assign across the first #residuals groups
      List<Integer> numInstances = new ArrayList<>();
      int instanceInd = 0;
      for (int groupInd = 0; groupInd < numGroups; groupInd++) {
        int num = groupInd < residuals
            ? instancesPerGroupBase + 1
            : instancesPerGroupBase;
        String groupId = computeVirtualGroupId(groupInd, virtualGroupName);
        assignment.put(groupId, new HashSet<>());
        for (int i = 0; i < num; i++) {
          assignment.get(groupId).add(sortedInstances.get(instanceInd));
          instanceInd++;
        }
        numInstances.add(num);
      }
      Preconditions.checkState(numInstances.stream().mapToInt(Integer::intValue).sum() == sortedInstances.size());
      return ImmutableMap.copyOf(assignment);
    }
  },

  /**
   * A round-robin strategy that assigns virtual groups based on mod.
   */
  ROUND_ROBIN {
    @Override
    public Map<String, Set<String>> computeAssignment(int numGroups, String virtualGroupName,
        Map<String, Set<String>> zoneMapping) {
      List<String> sortedInstances = HelixUtil.sortAndFlattenZoneMapping(zoneMapping);
      Map<String, Set<String>> assignment = new HashMap<>();
      int ind = 0;
      for (String instance : sortedInstances) {
        String groupId = VirtualTopologyGroupScheme.computeVirtualGroupId(ind % numGroups, virtualGroupName);
        assignment.putIfAbsent(groupId, new HashSet<>());
        assignment.get(groupId).add(instance);
        ind++;
      }
      return ImmutableMap.copyOf(assignment);
    }
  },

  /**
   * A strategy that combines FIFO and ROUND_ROBIN.
   * Given that instances.size = instancesPerGroup * numGroups + residuals,
   * we first densely assign [instancesPerGroup] instances per virtual group, then use round-robin to assign the
   * residuals that span across all groups.
   */
  FIFO_COMBINED_ROUND_ROBIN {
    @Override
    public Map<String, Set<String>> computeAssignment(
        int numGroups, String virtualGroupName, Map<String, Set<String>> zoneMapping) {
      List<String> sortedInstances = HelixUtil.sortAndFlattenZoneMapping(zoneMapping);
      Map<String, Set<String>> assignment = new HashMap<>();
      int instancesPerGroup = sortedInstances.size() / numGroups;

      // instances.size = instancePerGroup * numGroups + residual
      // step 1, continuously assign following groupInd order until entering residual section
      for (int instanceInd = 0; instanceInd < instancesPerGroup * numGroups; instanceInd++) {
        int groupIndex = instanceInd / instancesPerGroup;
        String groupId = computeVirtualGroupId(groupIndex, virtualGroupName);
        assignment.putIfAbsent(groupId, new HashSet<>());
        assignment.get(groupId).add(sortedInstances.get(instanceInd));
      }
      // step 2, assign the residuals, either stick to the last assigned group or round-robin to all groups.
      for (int instanceInd = instancesPerGroup * numGroups; instanceInd < sortedInstances.size(); instanceInd++) {
        int groupIndex = numGroups <= zoneMapping.keySet().size()
            ? numGroups - 1
            : instanceInd % numGroups;
        String groupId = computeVirtualGroupId(groupIndex, virtualGroupName);
        assignment.putIfAbsent(groupId, new HashSet<>());
        assignment.get(groupId).add(sortedInstances.get(instanceInd));
      }
      return ImmutableMap.copyOf(assignment);
    }
  };

  private static String computeVirtualGroupId(int groupIndex, String virtualGroupName) {
    return virtualGroupName + VirtualTopologyGroupConstants.GROUP_NAME_SPLITTER + groupIndex;
  }
}
