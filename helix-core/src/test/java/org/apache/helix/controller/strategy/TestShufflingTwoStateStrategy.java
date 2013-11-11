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

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestShufflingTwoStateStrategy {
  @Test()
  public void testInvocation() throws Exception {
    int partitions = 6, replicas = 2;
    String dbName = "espressoDB1";
    List<String> instanceNames = new ArrayList<String>();
    instanceNames.add("localhost_1231");
    instanceNames.add("localhost_1232");
    instanceNames.add("localhost_1233");
    instanceNames.add("localhost_1234");

    ZNRecord result =
        ShufflingTwoStateStrategy.calculateIdealState(instanceNames, partitions, replicas, dbName);
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "MASTER");
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "SLAVE");

    ZNRecord result2 =
        RUSHMasterSlaveStrategy.calculateIdealState(instanceNames, 1, partitions, replicas, dbName);

    ZNRecord result3 =
        ConsistentHashingMasterSlaveStrategy.calculateIdealState(instanceNames, partitions,
            replicas, dbName, new ConsistentHashingMasterSlaveStrategy.FnvHash());
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result3, "MASTER");
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result3, "SLAVE");
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result3, "");
    ConsistentHashingMasterSlaveStrategy.printNodeOfflineOverhead(result3);

    // System.out.println(result);
    ObjectMapper mapper = new ObjectMapper();

    // ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, result);
    // System.out.println(sw.toString());

    ZNRecord zn = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);
    System.out.println(result.toString());
    System.out.println(zn.toString());
    AssertJUnit.assertTrue(zn.toString().equalsIgnoreCase(result.toString()));
    System.out.println();

    sw = new StringWriter();
    mapper.writeValue(sw, result2);

    ZNRecord zn2 = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);
    System.out.println(result2.toString());
    System.out.println(zn2.toString());
    AssertJUnit.assertTrue(zn2.toString().equalsIgnoreCase(result2.toString()));

    sw = new StringWriter();
    mapper.writeValue(sw, result3);
    System.out.println();

    ZNRecord zn3 = mapper.readValue(new StringReader(sw.toString()), ZNRecord.class);
    System.out.println(result3.toString());
    System.out.println(zn3.toString());
    AssertJUnit.assertTrue(zn3.toString().equalsIgnoreCase(result3.toString()));
    System.out.println();

  }

  @Test
  public void testShuffledIdealState() {
    // partitions is larger than nodes
    int partitions = 6, replicas = 2, instances = 4;
    String dbName = "espressoDB1";
    List<String> instanceNames = new ArrayList<String>();
    instanceNames.add("localhost_1231");
    instanceNames.add("localhost_1232");
    instanceNames.add("localhost_1233");
    instanceNames.add("localhost_1234");

    ZNRecord result =
        ShufflingTwoStateStrategy.calculateIdealState(instanceNames, partitions, replicas, dbName);
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "MASTER");
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "SLAVE");
    Assert.assertTrue(verify(result));

    // partition is less than nodes
    instanceNames.clear();
    partitions = 4;
    replicas = 3;
    instances = 7;

    for (int i = 0; i < instances; i++) {
      instanceNames.add("localhost_" + (1231 + i));
    }
    result =
        ShufflingTwoStateStrategy.calculateIdealState(instanceNames, partitions, replicas, dbName);
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "MASTER");
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "SLAVE");
    Assert.assertTrue(verify(result));

    // partitions is multiple of nodes
    instanceNames.clear();
    partitions = 14;
    replicas = 3;
    instances = 7;

    for (int i = 0; i < instances; i++) {
      instanceNames.add("localhost_" + (1231 + i));
    }
    result =
        ShufflingTwoStateStrategy.calculateIdealState(instanceNames, partitions, replicas, dbName);
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "MASTER");
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "SLAVE");
    Assert.assertTrue(verify(result));

    // nodes are multiple of partitions
    instanceNames.clear();
    partitions = 4;
    replicas = 3;
    instances = 8;

    for (int i = 0; i < instances; i++) {
      instanceNames.add("localhost_" + (1231 + i));
    }
    result =
        ShufflingTwoStateStrategy.calculateIdealState(instanceNames, partitions, replicas, dbName);
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "MASTER");
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "SLAVE");
    Assert.assertTrue(verify(result));

    // nodes are multiple of partitions
    instanceNames.clear();
    partitions = 4;
    replicas = 3;
    instances = 12;

    for (int i = 0; i < instances; i++) {
      instanceNames.add("localhost_" + (1231 + i));
    }
    result =
        ShufflingTwoStateStrategy.calculateIdealState(instanceNames, partitions, replicas, dbName);
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "MASTER");
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "SLAVE");
    Assert.assertTrue(verify(result));

    // Just fits
    instanceNames.clear();
    partitions = 4;
    replicas = 2;
    instances = 12;

    for (int i = 0; i < instances; i++) {
      instanceNames.add("localhost_" + (1231 + i));
    }
    result =
        ShufflingTwoStateStrategy.calculateIdealState(instanceNames, partitions, replicas, dbName);
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "MASTER");
    ConsistentHashingMasterSlaveStrategy.printIdealStateStats(result, "SLAVE");
    Assert.assertTrue(verify(result));
  }

  boolean verify(ZNRecord result) {
    Map<String, Integer> masterPartitionCounts = new HashMap<String, Integer>();
    Map<String, Integer> slavePartitionCounts = new HashMap<String, Integer>();

    for (String key : result.getMapFields().keySet()) {
      Map<String, String> mapField = result.getMapField(key);
      int masterCount = 0;
      for (String host : mapField.keySet()) {
        if (mapField.get(host).equals("MASTER")) {
          Assert.assertTrue(masterCount == 0);
          masterCount++;
          if (!masterPartitionCounts.containsKey(host)) {
            masterPartitionCounts.put(host, 0);
          } else {
            masterPartitionCounts.put(host, masterPartitionCounts.get(host) + 1);
          }
        } else {
          if (!slavePartitionCounts.containsKey(host)) {
            slavePartitionCounts.put(host, 0);
          } else {
            slavePartitionCounts.put(host, slavePartitionCounts.get(host) + 1);
          }
        }
      }
    }

    List<Integer> masterCounts = new ArrayList<Integer>();
    List<Integer> slaveCounts = new ArrayList<Integer>();
    masterCounts.addAll(masterPartitionCounts.values());
    slaveCounts.addAll(slavePartitionCounts.values());
    Collections.sort(masterCounts);
    Collections.sort(slaveCounts);

    Assert.assertTrue(masterCounts.get(masterCounts.size() - 1) - masterCounts.get(0) <= 1);

    Assert.assertTrue(slaveCounts.get(slaveCounts.size() - 1) - slaveCounts.get(0) <= 2);
    return true;
  }
}
