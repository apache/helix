package org.apache.helix.view.dataprovider;

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
import java.util.Set;
import org.apache.helix.HelixConstants;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestViewClusterDataCache extends ZkTestBase {
  private static final int NUM_INSTANCE = 5;

  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private final Set<String> _instances = new HashSet<>();

  @BeforeClass
  public void beforeClass() {
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < NUM_INSTANCE; i++) {
      String instanceName = String.format("%s-%s-%s", CLUSTER_NAME, PARTICIPANT_PREFIX, i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
      _instances.add(instanceName);
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      participant.syncStart();
    }
  }

  @Test
  public void testCacheUpdate() {
    ViewClusterDataCache cache = new ViewClusterDataCache(CLUSTER_NAME);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    Assert.assertTrue(cache.updateCache(accessor));
    Assert.assertEquals(cache.getLiveInstances().size(), NUM_INSTANCE);
    Assert.assertEquals(cache.getInstanceConfigMap().size(), NUM_INSTANCE);
    for (String instance : _instances) {
      Assert.assertTrue(cache.getLiveInstances().containsKey(instance));
    }
    // update again, expect no cache change
    Assert.assertFalse(cache.updateCache(accessor));
    // clear cache and update again
    cache.clearCache(HelixConstants.ChangeType.LIVE_INSTANCE);
    cache.clearCache(HelixConstants.ChangeType.INSTANCE_CONFIG);
    Assert.assertTrue(cache.updateCache(accessor));
  }
}
