package org.apache.helix.controller.strategy;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixException;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.IdealState;

public class EspressoRelayStrategy {
  public static IdealState calculateRelayIdealState(List<String> partitions,
      List<String> instances, String resultRecordName, int replica, String firstValue,
      String restValue, String stateModelName) {
    Collections.sort(partitions);
    Collections.sort(instances);
    if (instances.size() % replica != 0) {
      throw new HelixException("Instances must be divided by replica");
    }

    IdealState result = new IdealState(resultRecordName);
    result.setNumPartitions(partitions.size());
    result.setReplicas("" + replica);
    result.setStateModelDefId(StateModelDefId.from(stateModelName));

    int groups = instances.size() / replica;
    int remainder = instances.size() % replica;

    int remainder2 = partitions.size() % groups;
    int storageNodeGroupSize = partitions.size() / groups;

    for (int i = 0; i < groups; i++) {
      int relayStart = 0, relayEnd = 0, storageNodeStart = 0, storageNodeEnd = 0;
      if (i < remainder) {
        relayStart = (replica + 1) * i;
        relayEnd = (replica + 1) * (i + 1);
      } else {
        relayStart = (replica + 1) * remainder + replica * (i - remainder);
        relayEnd = relayStart + replica;
      }
      // System.out.println("relay start :" + relayStart + " relayEnd:" + relayEnd);
      if (i < remainder2) {
        storageNodeStart = (storageNodeGroupSize + 1) * i;
        storageNodeEnd = (storageNodeGroupSize + 1) * (i + 1);
      } else {
        storageNodeStart =
            (storageNodeGroupSize + 1) * remainder2 + storageNodeGroupSize * (i - remainder2);
        storageNodeEnd = storageNodeStart + storageNodeGroupSize;
      }

      // System.out.println("storageNodeStart :" + storageNodeStart + " storageNodeEnd:" +
      // storageNodeEnd);
      List<String> snBatch = partitions.subList(storageNodeStart, storageNodeEnd);
      List<String> relayBatch = instances.subList(relayStart, relayEnd);

      Map<String, List<String>> sublistFields =
          calculateSubIdealState(snBatch, relayBatch, replica);

      result.getRecord().getListFields().putAll(sublistFields);
    }

    for (String snName : result.getRecord().getListFields().keySet()) {
      Map<String, String> mapField = new TreeMap<String, String>();
      List<String> relayCandidates = result.getRecord().getListField(snName);
      mapField.put(relayCandidates.get(0), firstValue);
      for (int i = 1; i < relayCandidates.size(); i++) {
        mapField.put(relayCandidates.get(i), restValue);
      }
      result.getRecord().getMapFields().put(snName, mapField);
    }
    System.out.println();
    return result;
  }

  private static Map<String, List<String>> calculateSubIdealState(List<String> snBatch,
      List<String> relayBatch, int replica) {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    for (int i = 0; i < snBatch.size(); i++) {
      String snName = snBatch.get(i);
      result.put(snName, new ArrayList<String>());
      for (int j = 0; j < replica; j++) {
        result.get(snName).add(relayBatch.get((j + i) % (relayBatch.size())));
      }
    }
    return result;
  }
}
