package org.apache.helix.controller;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState.CurrentStateProperty;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

/*
 * ZKRoutingInfoProvider keeps a copy of the routing table. Given a partition id,
 * it will return
 *
 * 1. The list of partition that can be read
 * 2. the master partition, for write operation
 *
 * The routing table is constructed from the currentState of each storage nodes.
 * The current state is a list of following pairs: partition-id:State(MASTER / SLAVE)
 *
 * TODO: move the code as part of router process
 * TODO: add listeners to node current state changes
 * */
public class ExternalViewGenerator {
  static Logger _logger = Logger.getLogger(ExternalViewGenerator.class);

  /*
   * Given a list of external view ZNRecord nodes(one for each cluster),
   * calculate the routing map.
   * The format of the routing map is like this:
   * Map<String, Map<String, Set<String>>> maps from a partitionName to its
   * states Map<String, List<String>> The second Map maps from a state
   * ("MASTER", "SLAVE"...) to a list of nodeNames
   * So that the we can query the map for the list of nodes by providing the
   * partition name and the expected state.
   */
  public Map<String, Map<String, Set<String>>> getRouterMapFromExternalView(
      List<ZNRecord> externalViewList) {
    Map<String, Map<String, Set<String>>> result = new TreeMap<String, Map<String, Set<String>>>();

    for (ZNRecord nodeView : externalViewList) {
      Map<String, Map<String, String>> partitionNodeStateMap = nodeView.getMapFields();
      for (String partitionId : partitionNodeStateMap.keySet()) {
        if (!result.containsKey(partitionId)) {
          result.put(partitionId, new TreeMap<String, Set<String>>());
        }
        Map<String, String> nodeStateMap = partitionNodeStateMap.get(partitionId);
        for (String nodeName : nodeStateMap.keySet()) {
          String state = nodeStateMap.get(nodeName);
          if (!result.get(partitionId).containsKey(state)) {
            result.get(partitionId).put(state, new TreeSet<String>());
          }
          result.get(partitionId).get(state).add(nodeName);
        }
      }
    }
    return result;
  }

  /*
   * The parameter is a map that maps the nodeName to a list of ZNRecords.
   */
  public List<ZNRecord> computeExternalView(Map<String, List<ZNRecord>> currentStates,
      List<ZNRecord> idealStates) {
    List<ZNRecord> resultList = new ArrayList<ZNRecord>();
    Map<String, ZNRecord> resultRoutingTable = new HashMap<String, ZNRecord>();
    // maps from resourceName to another map : partition -> map <nodename,
    // master/slave>;
    // Fill the routing table with "empty" default state according to ideals
    // states
    // in the cluster
    if (idealStates != null) {
      for (ZNRecord idealState : idealStates) {
        ZNRecord defaultExternalView = new ZNRecord(idealState.getId());
        resultRoutingTable.put(idealState.getId(), defaultExternalView);
      }
    } else {
      assert (!currentStates.isEmpty());
      return resultList;
    }
    for (String nodeName : currentStates.keySet()) {
      List<ZNRecord> znStates = currentStates.get(nodeName);
      for (ZNRecord nodeStateRecord : znStates) {
        Map<String, Map<String, String>> resourceStates = nodeStateRecord.getMapFields();
        for (String stateUnitKey : resourceStates.keySet()) {
          Map<String, String> partitionStates = resourceStates.get(stateUnitKey);
          String resourceName = partitionStates.get(Message.Attributes.RESOURCE_NAME.toString());
          ZNRecord partitionStatus = resultRoutingTable.get(resourceName);
          if (partitionStatus == null) {
            partitionStatus = new ZNRecord(resourceName);
            resultRoutingTable.put(resourceName, partitionStatus);
          }
          String currentStateKey = CurrentStateProperty.CURRENT_STATE.toString();

          if (!partitionStatus.getMapFields().containsKey(stateUnitKey)) {
            partitionStatus.setMapField(stateUnitKey, new TreeMap<String, String>());
          }
          partitionStatus.getMapField(stateUnitKey).put(nodeName,
              partitionStates.get(currentStateKey));

        }
      }
    }
    for (ZNRecord record : resultRoutingTable.values()) {
      resultList.add(record);
    }
    return resultList;
  }
}
