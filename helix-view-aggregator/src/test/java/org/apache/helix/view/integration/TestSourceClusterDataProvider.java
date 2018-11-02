package org.apache.helix.view.integration;

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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.helix.PropertyType;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.view.dataprovider.SourceClusterDataProvider;
import org.apache.helix.view.mock.MockClusterEventProcessor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSourceClusterDataProvider extends ViewAggregatorIntegrationTestBase {
  private static final int numSourceCluster = 1;
  private static final String stateModel = "MasterSlave";
  private static final String testResource = "testResource";

  @Test
  public void testSourceClusterDataProviderWatchAndRefresh() throws Exception {
    String clusterName = _allSourceClusters.get(0);
    List<PropertyType> properties = Arrays
        .asList(PropertyType.LIVEINSTANCES, PropertyType.EXTERNALVIEW, PropertyType.INSTANCES);

    ViewClusterSourceConfig sourceClusterConfig =
        new ViewClusterSourceConfig(clusterName, ZK_ADDR, properties);

    MockClusterEventProcessor processor = new MockClusterEventProcessor(clusterName);
    processor.start();

    SourceClusterDataProvider dataProvider =
        new SourceClusterDataProvider(sourceClusterConfig, processor);

    // setup can be re-called
    dataProvider.setup();
    dataProvider.setup();

    Assert.assertEquals(new HashSet<>(dataProvider.getPropertiesToAggregate()),
        new HashSet<>(properties));

    // When first connected, data provider will have some initial events
    Assert.assertEquals(processor.getHandledExternalViewChangeCount(), 1);
    Assert.assertEquals(processor.getHandledInstanceConfigChangeCount(), 1);
    Assert.assertEquals(processor.getHandledLiveInstancesChangeCount(), 1);
    processor.resetHandledEventCount();

    // ListNames should work
    Assert.assertEquals(dataProvider.getInstanceConfigNames().size(), numParticipant);
    Assert.assertEquals(dataProvider.getLiveInstanceNames().size(), numParticipant);
    Assert.assertEquals(dataProvider.getExternalViewNames().size(), 0);

    processor.resetHandledEventCount();

    // rebalance resource to check external view related events
    _gSetupTool.addResourceToCluster(clusterName, testResource, numParticipant, stateModel);
    _gSetupTool.rebalanceResource(clusterName, testResource, 3);
    Thread.sleep(1000);
    Assert.assertTrue(processor.getHandledExternalViewChangeCount() > 0);
    Assert.assertEquals(dataProvider.getExternalViewNames().size(), 1);

    // refresh data provider will have correct data loaded
    dataProvider.refreshCache();
    Assert.assertEquals(dataProvider.getLiveInstances().size(), numParticipant);
    Assert.assertEquals(dataProvider.getInstanceConfigMap().size(), numParticipant);
    Assert.assertEquals(dataProvider.getExternalViews().size(), 1);
    processor.resetHandledEventCount();

    // Add additional participant will have corresponding change
    String testParticipantName = "testParticipant";
    _gSetupTool.addInstanceToCluster(clusterName, testParticipantName);
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, clusterName, testParticipantName);
    participant.syncStart();

    Thread.sleep(500);
    Assert.assertEquals(processor.getHandledInstanceConfigChangeCount(), 1);
    Assert.assertEquals(processor.getHandledLiveInstancesChangeCount(), 1);

    // shutdown can be re-called
    dataProvider.shutdown();
    dataProvider.shutdown();

    // Verify cache is cleaned up
    Assert.assertEquals(dataProvider.getLiveInstances().size(), 0);
    Assert.assertEquals(dataProvider.getInstanceConfigMap().size(), 0);
    Assert.assertEquals(dataProvider.getExternalViews().size(), 0);
  }

  @Test
  public void testSourceClusterDataProviderPropertyFilter() throws Exception {
    String clusterName = _allSourceClusters.get(0);
    List<PropertyType> properties = Arrays.asList(
        new PropertyType[] { PropertyType.LIVEINSTANCES, PropertyType.EXTERNALVIEW });

    ViewClusterSourceConfig sourceClusterConfig =
        new ViewClusterSourceConfig(clusterName, ZK_ADDR, properties);

    MockClusterEventProcessor processor = new MockClusterEventProcessor(clusterName);
    processor.start();

    SourceClusterDataProvider dataProvider =
        new SourceClusterDataProvider(sourceClusterConfig, processor);
    dataProvider.setup();

    Assert.assertEquals(new HashSet<>(dataProvider.getPropertiesToAggregate()),
        new HashSet<>(properties));

    // When first connected, data provider will have some initial events, but InstanceConfig
    // will be filtered out since its not in properties
    Assert.assertEquals(processor.getHandledExternalViewChangeCount(), 1);
    Assert.assertEquals(processor.getHandledInstanceConfigChangeCount(), 0);
    Assert.assertEquals(processor.getHandledLiveInstancesChangeCount(), 1);

    dataProvider.shutdown();
  }

  @Override
  protected int getNumSourceCluster() {
    return numSourceCluster;
  }

}
