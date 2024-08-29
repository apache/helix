package org.apache.helix.gateway.utils;

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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.gateway.util.GatewayCurrentStateCache;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestGatewayCurrentStateCache {
  private GatewayCurrentStateCache cache = new GatewayCurrentStateCache("TestCluster");;

  @Test
  public void testUpdateCacheWithNewCurrentStateAndGetDiff() {
    Map<String, Map<String, String>> newState = new HashMap<>();
    Map<String, String> instanceState = new HashMap<>();
    instanceState.put("shard1", "ONLINE");
    instanceState.put("shard2", "OFFLINE");
    newState.put("instance1", instanceState);

    Map<String, Map<String, String>> diff = cache.updateCacheWithNewCurrentStateAndGetDiff(newState);

    Assert.assertNotNull(diff);
    Assert.assertEquals(diff.size(), 1);
    Assert.assertEquals(diff.get("instance1").size(), 2);
    Assert.assertEquals(diff.get("instance1").get("shard1"), "ONLINE");
    Assert.assertEquals(diff.get("instance1").get("shard2"), "OFFLINE");
    Assert.assertEquals(cache.getCurrentState("instance1", "shard1"), "ONLINE");
    Assert.assertEquals(cache.getCurrentState("instance1", "shard2"), "OFFLINE");
  }

  @Test(dependsOnMethods = "testUpdateCacheWithNewCurrentStateAndGetDiff")
  public void testUpdateCacheWithCurrentStateDiff() {
    Map<String, Map<String, String>> diff = new HashMap<>();
    Map<String, String> instanceState = new HashMap<>();
    instanceState.put("shard1", "OFFLINE");
    diff.put("instance1", instanceState);

    cache.updateCacheWithCurrentStateDiff(diff);
    Assert.assertEquals(cache.getCurrentState("instance1", "shard1"), "OFFLINE");
    Assert.assertEquals(cache.getCurrentState("instance1", "shard2"), "OFFLINE");
  }

  @Test(dependsOnMethods = "testUpdateCacheWithCurrentStateDiff")
  public void testUpdateTargetStateWithDiff() {
    Map<String, Map<String, String>> targetStateChange = new HashMap<>();
    Map<String, String> instanceState = new HashMap<>();
    instanceState.put("shard1", "ONLINE");
    targetStateChange.put("instance1", instanceState);

    cache.updateTargetStateWithDiff(targetStateChange);

    Assert.assertTrue(cache.isTargetStateChanged());
    Assert.assertEquals(cache.getTargetStateMap().get("instance1").get("shard1"), "ONLINE");
  }

  @Test(dependsOnMethods = "testUpdateTargetStateWithDiff")
  public void testSerializeTargetAssignments() {
    Map<String, Map<String, String>> targetState = new HashMap<>();
    Map<String, String> instanceState = new HashMap<>();
    instanceState.put("shard1", "OFFLINE");
    targetState.put("instance1", instanceState);

    cache.updateTargetStateWithDiff(targetState);

    String serialized = cache.serializeTargetAssignments();
    Assert.assertTrue(serialized.contains("\"instance1\":{\"shard1\":\"OFFLINE\"}"));
  }
}
