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
import java.util.Arrays;
import java.util.List;
import org.apache.helix.PropertyType;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.view.aggregator.SourceClusterConfigChangeAction;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSourceClusterConfigChangeAction {
  private static final String viewClusterName = "testSourceClusterConfigChangeAction-viewCluster";
  private static final String testZkAddr = "localhost:5050";

  @Test
  public void testActionComputationOnStartup() {
    ClusterConfig config = DataProviderTestUtil.createDefaultViewClusterConfig(viewClusterName, 2);
    SourceClusterConfigChangeAction action = new SourceClusterConfigChangeAction(null, config);
    action.computeAction();
    Assert.assertEquals(action.getConfigsToAdd().size(),
        config.getViewClusterSourceConfigs().size());
    Assert.assertEquals(action.getConfigsToDelete().size(), 0);
    Assert.assertTrue(action.shouldResetTimer());
    Assert.assertEquals(action.getCurrentRefreshPeriodMs(),
        config.getViewClusterRefershPeriod() * 1000);
    Assert.assertTrue(compareSourceClusterConfigs(action.getConfigsToAdd(),
        config.getViewClusterSourceConfigs()));
  }

  @Test
  public void testActionComputationOnConfigModified() {
    ClusterConfig config = DataProviderTestUtil.createDefaultViewClusterConfig(viewClusterName, 2);
    ClusterConfig newConfig = new ClusterConfig(viewClusterName);
    newConfig.setViewCluster();

    List<PropertyType> newProperties =
        Arrays.asList(new PropertyType[] { PropertyType.LIVEINSTANCES, PropertyType.EXTERNALVIEW });
    List<ViewClusterSourceConfig> newSourceConfigs =
        new ArrayList<>(config.getViewClusterSourceConfigs());
    newConfig.setViewClusterRefreshPeriod(config.getViewClusterRefershPeriod());

    // Remove one source cluster, and add another source cluster
    ViewClusterSourceConfig removed = newSourceConfigs.get(0);
    newSourceConfigs.remove(0);
    newSourceConfigs.add(new ViewClusterSourceConfig("cluster3", testZkAddr, newProperties));
    newConfig.setViewClusterSourceConfigs(newSourceConfigs);

    SourceClusterConfigChangeAction action = new SourceClusterConfigChangeAction(config, newConfig);
    action.computeAction();
    Assert.assertEquals(action.getConfigsToDelete().size(), 1);
    Assert.assertEquals(action.getConfigsToAdd().size(), 1);
    Assert.assertEquals(action.getConfigsToDelete().get(0), removed);
    Assert.assertEquals(action.getConfigsToAdd().get(0),
        newConfig.getViewClusterSourceConfigs().get(1));

    // modify one source cluster
    newSourceConfigs = new ArrayList<>(config.getViewClusterSourceConfigs());
    newSourceConfigs.get(0).setProperties(newProperties);
    newConfig.setViewClusterSourceConfigs(newSourceConfigs);
    action = new SourceClusterConfigChangeAction(config, newConfig);
    action.computeAction();

    Assert.assertEquals(action.getConfigsToDelete().size(), 1);
    Assert.assertEquals(action.getConfigsToAdd().size(), 1);
    Assert.assertEquals(action.getConfigsToDelete().get(0),
        config.getViewClusterSourceConfigs().get(0));
    Assert.assertEquals(action.getConfigsToAdd().get(0),
        newConfig.getViewClusterSourceConfigs().get(0));
  }

  @Test
  public void testActionComputationNoChange() {
    ClusterConfig config = DataProviderTestUtil.createDefaultViewClusterConfig(viewClusterName, 2);
    SourceClusterConfigChangeAction action = new SourceClusterConfigChangeAction(config, config);
    Assert.assertEquals(action.getConfigsToAdd().size(), 0);
    Assert.assertEquals(action.getConfigsToDelete().size(), 0);
    Assert.assertFalse(action.shouldResetTimer());
  }

  @Test
  public void testActionComputationInvalidInitialization() {
    ClusterConfig config = DataProviderTestUtil.createDefaultViewClusterConfig(viewClusterName, 2);
    ClusterConfig badConfig = new ClusterConfig(viewClusterName);
    try {
      SourceClusterConfigChangeAction action = new SourceClusterConfigChangeAction(config, null);
      Assert.assertFalse(true, "New config should not be null");
    } catch (IllegalArgumentException e) {
      // OK
    }

    try {
      SourceClusterConfigChangeAction action =
          new SourceClusterConfigChangeAction(badConfig, config);
      Assert.assertFalse(true, "Old config should be view cluster config");
    } catch (IllegalArgumentException e) {
      // OK
    }

    try {
      SourceClusterConfigChangeAction action =
          new SourceClusterConfigChangeAction(config, badConfig);
      Assert.assertFalse(true, "New config should be view cluster config");
    } catch (IllegalArgumentException e) {
      // OK
    }
  }

  private static boolean compareSourceClusterConfigs(List<ViewClusterSourceConfig> list1,
      List<ViewClusterSourceConfig> list2) {
    return list1.containsAll(list2) && list2.containsAll(list1);
  }
}
