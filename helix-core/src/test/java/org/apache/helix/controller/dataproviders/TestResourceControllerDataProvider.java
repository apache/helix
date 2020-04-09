package org.apache.helix.controller.dataproviders;

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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResourceControllerDataProvider {
  @Test
  public void testStablePartitionListCache() {
    ResourceControllerDataProvider dataProvider = new ResourceControllerDataProvider();
    String resourceName = "TestResource";
    Set<String> partitionSetA = ImmutableSet.of("Partiton1", "Partiton2", "Partiton3");
    Set<String> partitionSetB = ImmutableSet.of("Partiton1", "Partiton2", "Partiton4");

    // 1. Test get the stable list
    List<String> cachedPartitionListA =
        dataProvider.getOrSetStablePartitionList(resourceName, partitionSetA);
    Assert.assertTrue(cachedPartitionListA.size() == partitionSetA.size() && cachedPartitionListA
        .containsAll(partitionSetA));

    Set<String> partitionSetAWithDifferentOrder = new LinkedHashSet<>();
    partitionSetAWithDifferentOrder.add("Partiton3");
    partitionSetAWithDifferentOrder.add("Partiton2");
    partitionSetAWithDifferentOrder.add("Partiton1");
    // Verify that iterating this list will generate a different result
    List<String> tmpPartitionList = new ArrayList<>(partitionSetAWithDifferentOrder);
    Assert.assertFalse(cachedPartitionListA.equals(tmpPartitionList));
    // Verify that the cached stable partition list still return a list with the same order.
    Assert.assertTrue(
        dataProvider.getOrSetStablePartitionList(resourceName, partitionSetAWithDifferentOrder)
            .equals(cachedPartitionListA));

    // 2. Test update
    List<String> cachedPartitionListB =
        dataProvider.getOrSetStablePartitionList(resourceName, partitionSetB);
    Assert.assertTrue(cachedPartitionListB.size() == partitionSetB.size() && cachedPartitionListB
        .containsAll(partitionSetB));

    // 3. Test retain
    dataProvider.retainStablePartitionListCache(Collections.emptySet());
    // Now, since the cache has been cleaned, the get will return different order.
    Assert.assertFalse(
        dataProvider.getOrSetStablePartitionList(resourceName, partitionSetAWithDifferentOrder)
            .equals(cachedPartitionListA));
  }
}
