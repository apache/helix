package org.apache.helix.gateway.util;

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
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestGatewayCurrentStateCache {
  private GatewayCurrentStateCache cache;

  @BeforeMethod
  public void setUp() {
    cache = new GatewayCurrentStateCache("TestCluster");
  }

  @Test
  public void testUpdateCacheWithNewCurrentStateAndGetDiff() {
    Map<String, Map<String, Map<String, String>>> newState = new HashMap<>();
    Map<String, Map<String, String>> instanceState = new HashMap<>();
    Map<String, String> shardState = new HashMap<>();
    shardState.put("shard1", "ONLINE");
    instanceState.put("resource1", shardState);
    newState.put("instance1", instanceState);

    Map<String, Map<String, Map<String, String>>> diff = cache.updateCacheWithNewCurrentStateAndGetDiff(newState);

    Assert.assertNotNull(diff);
    Assert.assertEquals(diff.size(), 1);
    Assert.assertEquals(diff.get("instance1").get("resource1").get("shard1"), "ONLINE");
  }

  @Test
  public void testUpdateCacheWithCurrentStateDiff() {
    Map<String, Map<String, String>> instanceState = new HashMap<>();
    Map<String, String> shardState = new HashMap<>();
    shardState.put("shard2", "ONLINE");
    shardState.put("shard1", "ONLINE");
    instanceState.put("resource1", shardState);

    cache.updateCurrentStateOfExistingInstance("instance1", "resource1", "shard1", "ONLINE");

    Assert.assertEquals(cache.getCurrentState("instance1", "resource1", "shard1"), "ONLINE");
    Assert.assertEquals(cache.getCurrentState("instance1", "resource1", "shard2"), "ONLINE");
  }

  @Test
  public void testUpdateTargetStateWithDiff() {
    Map<String, Map<String, String>> targetStateChange = new HashMap<>();
    Map<String, String> shardState = new HashMap<>();
    shardState.put("shard1", "OFFLINE");
    targetStateChange.put("resource1", shardState);

    cache.updateTargetStateWithDiff("instance1", targetStateChange);

    Assert.assertEquals(cache.getTargetState("instance1", "resource1", "shard1"), "OFFLINE");
    Assert.assertEquals(cache.serializeTargetAssignmentsToJSONNode().toString(), "{\"instance1\":{\"resource1\":{\"shard1\":\"OFFLINE\"}}}");
  }
}
