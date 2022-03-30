package org.apache.helix.view.aggregator;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.MockAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.view.dataprovider.SourceClusterDataProvider;
import org.apache.helix.view.mock.MockSourceClusterDataProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestViewClusterRefresher {
  private static final String viewClusterName = "viewCluster";
  private static final int numSourceCluster = 3;
  private static final int numInstancePerSourceCluster = 2;
  private static final int numExternalViewPerSourceCluster = 3;
  private static final int numPartition = 3;
  private static final List<PropertyType> defaultProperties =
      Arrays.asList(PropertyType.LIVEINSTANCES, PropertyType.INSTANCES, PropertyType.EXTERNALVIEW);

  private class CounterBasedMockAccessor extends MockAccessor {
    private int _setCount;
    private int _removeCount;

    public CounterBasedMockAccessor(String clusterName) {
      super(clusterName);
      resetCounters();
    }

    public void resetCounters() {
      _setCount = 0;
      _removeCount = 0;
    }

    @Override
    public boolean setProperty(PropertyKey key, HelixProperty value) {
      _setCount += 1;
      return super.setProperty(key, value);
    }

    @Override
    public boolean removeProperty(PropertyKey key) {
      _removeCount += 1;
      return super.removeProperty(key);
    }

    public int getSetCount() {
      return _setCount;
    }

    public int getRemoveCount() {
      return _removeCount;
    }
  }

  @Test
  public void testRefreshWithNoChange() {
    CounterBasedMockAccessor viewClusterDataAccessor =
        new CounterBasedMockAccessor(viewClusterName);
    Map<String, SourceClusterDataProvider> dataProviderMap = new HashMap<>();
    createMockDataProviders(dataProviderMap);

    ViewClusterRefresher refresher =
        new ViewClusterRefresher(viewClusterName, viewClusterDataAccessor);
    refresher.updateProviderView(new HashSet<>(dataProviderMap.values()));

    // Refresh an empty view cluster
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.LIVEINSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.INSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.EXTERNALVIEW));
    viewClusterDataAccessor.resetCounters();

    // Refresh again without change
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.LIVEINSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.INSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.EXTERNALVIEW));
    Assert.assertEquals(viewClusterDataAccessor.getSetCount(), 0);
  }

  @Test
  public void testRefreshWithInstanceChange() {
    CounterBasedMockAccessor viewClusterDataAccessor =
        new CounterBasedMockAccessor(viewClusterName);
    Map<String, SourceClusterDataProvider> dataProviderMap = new HashMap<>();
    createMockDataProviders(dataProviderMap);

    ViewClusterRefresher refresher =
        new ViewClusterRefresher(viewClusterName, viewClusterDataAccessor);
    refresher.updateProviderView(new HashSet<>(dataProviderMap.values()));
    MockSourceClusterDataProvider sampleProvider =
        (MockSourceClusterDataProvider) dataProviderMap.get("cluster0");

    // Refresh an empty view cluster
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.LIVEINSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.INSTANCES));
    // multiply by 2 for both instance config and live instances
    Assert.assertEquals(viewClusterDataAccessor.getSetCount(),
        numSourceCluster * numInstancePerSourceCluster * 2);
    verifyInstances(viewClusterDataAccessor, dataProviderMap);

    // Source cluster has instances deleted
    viewClusterDataAccessor.resetCounters();
    sampleProvider.clearCache(HelixConstants.ChangeType.LIVE_INSTANCE);
    sampleProvider.clearCache(HelixConstants.ChangeType.INSTANCE_CONFIG);
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.LIVEINSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.INSTANCES));
    Assert.assertEquals(viewClusterDataAccessor.getRemoveCount(), numInstancePerSourceCluster * 2);

    verifyInstances(viewClusterDataAccessor, dataProviderMap);

    // Source cluster has live instances added
    viewClusterDataAccessor.resetCounters();
    List<LiveInstance> liveInstances =
        new ArrayList<>(sampleProvider.getLiveInstances().values());
    liveInstances.add(new LiveInstance("newLiveInstance"));
    sampleProvider.setLiveInstances(liveInstances);
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.LIVEINSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.INSTANCES));
    Assert.assertEquals(viewClusterDataAccessor.getSetCount(), 1);

    verifyInstances(viewClusterDataAccessor, dataProviderMap);
  }

  @Test
  public void testRefreshWithExternalViewChange() {
    CounterBasedMockAccessor accessor =
        new CounterBasedMockAccessor(viewClusterName);
    Map<String, SourceClusterDataProvider> dataProviderMap = new HashMap<>();
    createMockDataProviders(dataProviderMap);

    ViewClusterRefresher refresher =
        new ViewClusterRefresher(viewClusterName, accessor);
    refresher.updateProviderView(new HashSet<>(dataProviderMap.values()));
    MockSourceClusterDataProvider sampleProvider =
        (MockSourceClusterDataProvider) dataProviderMap.get("cluster0");

    // Refresh an empty view cluster
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.EXTERNALVIEW));
    // We only have 2 resource names so should be setting 2 evs
    Assert.assertEquals(accessor.getSetCount(), numExternalViewPerSourceCluster);
    Assert.assertEquals(accessor.getRemoveCount(), 0);
    // Partition count should be maintained
    // Since we are creating 1 replica per source cluster so replica should be aggregated.
    verifyExternalView(accessor, numExternalViewPerSourceCluster, numPartition, numSourceCluster);

    // One cluster deletes all its EVs, number of partition should reflect in changes
    accessor.resetCounters();
    sampleProvider.clearCache(HelixConstants.ChangeType.EXTERNAL_VIEW);
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.EXTERNALVIEW));
    Assert.assertEquals(accessor.getSetCount(), numExternalViewPerSourceCluster);
    Assert.assertEquals(accessor.getRemoveCount(), 0);
    verifyExternalView(accessor, numExternalViewPerSourceCluster, numPartition,
        numSourceCluster - 1);

    // All EVs are deleted, and all EVs should be removed from view cluster
    for (SourceClusterDataProvider provider : dataProviderMap.values()) {
      provider.clearCache(HelixConstants.ChangeType.EXTERNAL_VIEW);
    }
    accessor.resetCounters();
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.EXTERNALVIEW));
    Assert.assertEquals(accessor.getSetCount(), 0);
    Assert.assertEquals(accessor.getRemoveCount(), numExternalViewPerSourceCluster);
    verifyExternalView(accessor, 0, 0, 0);
  }

  @Test
  public void testRefreshWithProviderChange() {
    CounterBasedMockAccessor viewClusterDataAccessor =
        new CounterBasedMockAccessor(viewClusterName);
    Map<String, SourceClusterDataProvider> dataProviderMap = new HashMap<>();
    createMockDataProviders(dataProviderMap);

    ViewClusterRefresher refresher =
        new ViewClusterRefresher(viewClusterName, viewClusterDataAccessor);
    refresher.updateProviderView(new HashSet<>(dataProviderMap.values()));
    MockSourceClusterDataProvider sampleProvider =
        (MockSourceClusterDataProvider) dataProviderMap.get("cluster0");

    // Refresh an empty view cluster
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.LIVEINSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.INSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.EXTERNALVIEW));
    verifyInstances(viewClusterDataAccessor, dataProviderMap);
    verifyExternalView(viewClusterDataAccessor, numExternalViewPerSourceCluster, numPartition,
        numSourceCluster);
    viewClusterDataAccessor.resetCounters();

    // remove InstanceConfig and ExternalView requirement from sample provider
    sampleProvider.getConfig()
        .setProperties(Collections.singletonList(PropertyType.LIVEINSTANCES));

    // Refresh again
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.LIVEINSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.INSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.EXTERNALVIEW));

    // Removing external view from config of 1 cluster will not result in removing any external view
    // but to update external views
    Assert.assertEquals(viewClusterDataAccessor.getSetCount(), numExternalViewPerSourceCluster);
    Assert.assertEquals(viewClusterDataAccessor.getRemoveCount(), numInstancePerSourceCluster);
    verifyExternalView(viewClusterDataAccessor, numExternalViewPerSourceCluster, numPartition,
        numSourceCluster - 1);
    verifyInstances(viewClusterDataAccessor, dataProviderMap);

    // Remove external view from all source clusters, will resulting in removing all external views
    viewClusterDataAccessor.resetCounters();
    for (SourceClusterDataProvider provider : dataProviderMap.values()) {
      MockSourceClusterDataProvider mockProvider = (MockSourceClusterDataProvider) provider;
      if (mockProvider != sampleProvider) {
        mockProvider.getConfig().setProperties(Arrays
            .asList(PropertyType.LIVEINSTANCES, PropertyType.INSTANCES));
      }
    }

    // Refresh again
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.LIVEINSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.INSTANCES));
    Assert.assertTrue(refresher.refreshPropertiesInViewCluster(PropertyType.EXTERNALVIEW));

    // No change in instances, but external views are all removed
    Assert.assertEquals(viewClusterDataAccessor.getSetCount(), 0);
    Assert.assertEquals(viewClusterDataAccessor.getRemoveCount(), numExternalViewPerSourceCluster);
    verifyExternalView(viewClusterDataAccessor, 0, 0, 0);
    verifyInstances(viewClusterDataAccessor, dataProviderMap);
  }

  private void verifyExternalView(HelixDataAccessor accessor, int expectedResourceCnt,
      int expectedPartitionPerResource, int expectedReplicaPerPartition) {
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertEquals(accessor.getChildNames(keyBuilder.externalViews()).size(),
        expectedResourceCnt);
    for (String resourceName : accessor.getChildNames(keyBuilder.externalViews())) {
      ExternalView ev = accessor.getProperty(keyBuilder.externalView(resourceName));

      Assert.assertEquals(ev.getPartitionSet().size(), expectedPartitionPerResource);
      for (String partitionName : ev.getPartitionSet()) {
        Assert.assertEquals(ev.getStateMap(partitionName).size(), expectedReplicaPerPartition);
      }
    }
  }

  private void verifyInstances(HelixDataAccessor accessor,
      Map<String, SourceClusterDataProvider> dataProviderMap) {
    // Verify live instances - since we don't modify ZNode's internal content,
    // we just verify names
    Set<String> fetchedNames = new HashSet<>(accessor
        .getChildNames(accessor.keyBuilder().liveInstances()));
    Set<String> expectedNames = new HashSet<>();
    for (SourceClusterDataProvider provider : dataProviderMap.values()) {
      if (provider.getPropertiesToAggregate().contains(PropertyType.LIVEINSTANCES)) {
        expectedNames.addAll(provider.getLiveInstanceNames());
      }
    }
    Assert.assertEquals(fetchedNames, expectedNames);

    // Verify instance configs - since we don't modify ZNode's internal content,
    // we just verify names
    fetchedNames = new HashSet<>(accessor
        .getChildNames(accessor.keyBuilder().instanceConfigs()));
    expectedNames = new HashSet<>();
    for (SourceClusterDataProvider provider : dataProviderMap.values()) {
      if (provider.getPropertiesToAggregate().contains(PropertyType.INSTANCES)) {
        expectedNames.addAll(provider.getInstanceConfigNames());
      }
    }
    Assert.assertEquals(fetchedNames, expectedNames);
  }

  private void createMockDataProviders(Map<String, SourceClusterDataProvider> dataProviderMap) {
    for (int i = 0; i < numSourceCluster; i++) {
      String sourceClusterName = "cluster" + i;
      ViewClusterSourceConfig sourceConfig =
          new ViewClusterSourceConfig(sourceClusterName, "", defaultProperties);
      MockSourceClusterDataProvider provider =
          new MockSourceClusterDataProvider(sourceConfig, null);
      List<LiveInstance> liveInstanceList = new ArrayList<>();
      List<InstanceConfig> instanceConfigList = new ArrayList<>();
      List<ExternalView> externalViewList = new ArrayList<>();

      // InstanceConfig and LiveInstance
      for (int j = 0; j < numInstancePerSourceCluster; j++) {
        String instanceName = String.format("%s-instance%s", sourceClusterName, j);
        liveInstanceList.add(new LiveInstance(instanceName));
        instanceConfigList.add(new InstanceConfig(instanceName));
      }
      provider.setLiveInstances(liveInstanceList);
      provider.setInstanceConfigs(instanceConfigList);

      // ExternalView
      for (int j = 0; j < numExternalViewPerSourceCluster; j++) {
        String resourceName = String.format("Resource%s", j);
        ExternalView ev = new ExternalView(resourceName);
        for (int k = 0; k < numPartition; k++) {
          String partitionName = String.format("Partition%s", k);
          Map<String, String> stateMap = new HashMap<>();
          stateMap.put(String.format("%s-instance", sourceClusterName), "MASTER");
          ev.setStateMap(partitionName, stateMap);
        }
        externalViewList.add(ev);
      }
      provider.setExternalViews(externalViewList);
      dataProviderMap.put(sourceClusterName, provider);
    }
  }
}
