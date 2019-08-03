package org.apache.helix.controller.rebalancer.waged.model;

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

import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestAssignableReplica {
  String resourceName = "Resource";
  String partitionNamePrefix = "partition";
  String masterState = "Master";
  int masterPriority = StateModelDefinition.TOP_STATE_PRIORITY;
  String slaveState = "Slave";
  int slavePriority = 2;

  @Test
  public void testConstructRepliaWithResourceConfig() throws IOException {
    // Init assignable replica with a basic config object
    Map<String, Integer> capacityDataMapResource1 = new HashMap<>();
    capacityDataMapResource1.put("item1", 3);
    capacityDataMapResource1.put("item2", 6);
    ResourceConfig testResourceConfigResource = new ResourceConfig(resourceName);
    testResourceConfigResource.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, capacityDataMapResource1));

    String partitionName = partitionNamePrefix + 1;
    AssignableReplica replica =
        new AssignableReplica(testResourceConfigResource, partitionName, masterState,
            masterPriority);
    Assert.assertEquals(replica.getResourceName(), resourceName);
    Assert.assertEquals(replica.getPartitionName(), partitionName);
    Assert.assertEquals(replica.getReplicaState(), masterState);
    Assert.assertEquals(replica.getStatePriority(), masterPriority);
    Assert.assertTrue(replica.isReplicaTopState());
    Assert.assertEquals(replica.getCapacity(), capacityDataMapResource1);
    Assert.assertEquals(replica.getResourceInstanceGroupTag(), null);
    Assert.assertEquals(replica.getResourceMaxPartitionsPerInstance(), Integer.MAX_VALUE);

    // Modify the config and initialize more replicas.
    // 1. update capacity
    Map<String, Integer> capacityDataMapResource2 = new HashMap<>();
    capacityDataMapResource2.put("item1", 5);
    capacityDataMapResource2.put("item2", 10);
    Map<String, Map<String, Integer>> capacityMap =
        testResourceConfigResource.getPartitionCapacityMap();
    String partitionName2 = partitionNamePrefix + 2;
    capacityMap.put(partitionName2, capacityDataMapResource2);
    testResourceConfigResource.setPartitionCapacityMap(capacityMap);
    // 2. update instance group tag and max partitions per instance
    String group = "DEFAULT";
    int maxPartition = 10;
    testResourceConfigResource.getRecord()
        .setSimpleField(ResourceConfig.ResourceConfigProperty.INSTANCE_GROUP_TAG.toString(), group);
    testResourceConfigResource.getRecord()
        .setIntField(ResourceConfig.ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name(),
            maxPartition);

    replica = new AssignableReplica(testResourceConfigResource, partitionName, masterState,
        masterPriority);
    Assert.assertEquals(replica.getCapacity(), capacityDataMapResource1);
    Assert.assertEquals(replica.getResourceInstanceGroupTag(), group);
    Assert.assertEquals(replica.getResourceMaxPartitionsPerInstance(), maxPartition);

    replica = new AssignableReplica(testResourceConfigResource, partitionName2, slaveState,
        slavePriority);
    Assert.assertEquals(replica.getResourceName(), resourceName);
    Assert.assertEquals(replica.getPartitionName(), partitionName2);
    Assert.assertEquals(replica.getReplicaState(), slaveState);
    Assert.assertEquals(replica.getStatePriority(), slavePriority);
    Assert.assertFalse(replica.isReplicaTopState());
    Assert.assertEquals(replica.getCapacity(), capacityDataMapResource2);
    Assert.assertEquals(replica.getResourceInstanceGroupTag(), group);
    Assert.assertEquals(replica.getResourceMaxPartitionsPerInstance(), maxPartition);
  }
}
