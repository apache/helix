package org.apache.helix.integration;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBasicSpectator extends ZkStandAloneCMTestBase implements
    ExternalViewChangeListener {
  Map<String, Integer> _externalViewChanges = new HashMap<String, Integer>();

  @Test
  public void testSpectator() throws Exception {
    HelixManager relayHelixManager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, null, InstanceType.SPECTATOR, _zkaddr);

    relayHelixManager.connect();
    relayHelixManager.addExternalViewChangeListener(this);

    _setupTool.addResourceToCluster(CLUSTER_NAME, "NextDB", 64, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "NextDB", 3);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

    Assert.assertTrue(_externalViewChanges.containsKey("NextDB"));
    Assert.assertTrue(_externalViewChanges.containsKey(TEST_DB));

  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    for (ExternalView view : externalViewList) {
      if (!_externalViewChanges.containsKey(view.getResourceName())) {
        _externalViewChanges.put(view.getResourceName(), 1);
      } else {
        _externalViewChanges.put(view.getResourceName(),
            _externalViewChanges.get(view.getResourceName()) + 1);
      }
    }
  }
}
