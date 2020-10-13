package org.apache.helix.controller.rebalancer.waged;

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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.management.AttributeNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.rebalancer.waged.constraints.MockRebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.AbstractTestClusterModel;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.mock.MockManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.InstanceMonitor;
import org.apache.helix.monitoring.metrics.MetricCollector;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.monitoring.metrics.model.RatioMetric;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class TestWagedRebalancerMetrics extends AbstractTestClusterModel {
  private static final String TEST_STRING = "TEST";
  private MetricCollector _metricCollector;
  private Set<String> _instances;
  private MockRebalanceAlgorithm _algorithm;
  private MockAssignmentMetadataStore _metadataStore;

  @BeforeClass
  public void initialize() {
    super.initialize();
    _instances = new HashSet<>();
    _instances.add(_testInstanceId);
    _algorithm = new MockRebalanceAlgorithm();

    // Initialize a mock assignment metadata store
    _metadataStore = new MockAssignmentMetadataStore();
  }

  @Test
  public void testMetricValuePropagation()
      throws JMException, HelixRebalanceException, IOException {
    _metadataStore.reset();
    _metricCollector = new WagedRebalancerMetricCollector(TEST_STRING);
    WagedRebalancer rebalancer =
        new WagedRebalancer(_metadataStore, _algorithm, Optional.of(_metricCollector));

    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().stream()
              .forEach(partition -> resource.addPartition(partition));
          return resource;
        }));
    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());

    // Check that there exists a non-zero value in the metrics
    Assert.assertTrue(_metricCollector.getMetricMap().values().stream()
        .anyMatch(metric -> (long) metric.getLastEmittedMetricValue() > 0L));
  }

  @Test
  public void testWagedRebalanceMetrics()
      throws Exception {
    _metadataStore.reset();
    MetricCollector metricCollector = new WagedRebalancerMetricCollector(TEST_STRING);
    WagedRebalancer rebalancer =
        new WagedRebalancer(_metadataStore, _algorithm, Optional.of(metricCollector));
    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().stream()
              .forEach(partition -> resource.addPartition(partition));
          return resource;
        }));

    Assert.assertEquals((long) metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.GlobalBaselineCalcCounter.name(),
        CountMetric.class).getLastEmittedMetricValue(), 0L);
    Assert.assertEquals((long) metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceCounter.name(),
        CountMetric.class).getLastEmittedMetricValue(), 0L);
    Assert.assertEquals((double) metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.BaselineDivergenceGauge.name(),
        RatioMetric.class).getLastEmittedMetricValue(), 0.0d);

    // Cluster config change will trigger baseline recalculation and partial rebalance.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    // Add a field to the cluster config so the cluster config will be marked as changed in the change detector.
    clusterData.getClusterConfig().getRecord().setSimpleField("foo", "bar");

    rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());

    Assert.assertEquals((long) metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.GlobalBaselineCalcCounter.name(),
        CountMetric.class).getLastEmittedMetricValue(), 1L);
    Assert.assertEquals((long) metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceCounter.name(),
        CountMetric.class).getLastEmittedMetricValue(), 1L);

    // Wait for asyncReportBaselineDivergenceGauge to complete and verify.
    Assert.assertTrue(TestHelper.verify(() -> (double) metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.BaselineDivergenceGauge.name(),
        RatioMetric.class).getLastEmittedMetricValue() == 0.0d, TestHelper.WAIT_DURATION));
  }

  /*
   * Integration test for WAGED instance capacity metrics.
   */
  @Test
  public void testInstanceCapacityMetrics() throws Exception {
    final String clusterName = TestHelper.getTestMethodName();
    final ClusterStatusMonitor monitor = new ClusterStatusMonitor(clusterName);
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);

    ResourceControllerDataProvider cache = setupClusterDataCache();
    Map<String, Resource> resourceMap = cache.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));

    event.addAttribute(AttributeName.helixmanager.name(), new MockManager());
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), monitor);

    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ReadClusterDataStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.handle(event);

    final MBeanServerConnection mBeanServer = ManagementFactory.getPlatformMBeanServer();

    for (String instance : _instances) {
      String instanceBeanName = String.format("%s=%s,instanceName=%s",
          ClusterStatusMonitor.CLUSTER_DN_KEY, clusterName, instance);
      ObjectName instanceObjectName = monitor.getObjectName(instanceBeanName);

      Assert.assertTrue(TestHelper
          .verify(() -> mBeanServer.isRegistered(instanceObjectName),
              TestHelper.WAIT_DURATION));

      // Verify capacity gauge metrics
      for (Map.Entry<String, Integer> capacityEntry : _capacityDataMap.entrySet()) {
        String capacityKey = capacityEntry.getKey();
        String attributeName = capacityKey + "Gauge";
        Assert.assertTrue(TestHelper.verify(() -> {
          try {
            return (long) mBeanServer.getAttribute(instanceObjectName, attributeName)
                == _capacityDataMap.get(capacityKey);
          } catch (AttributeNotFoundException e) {
            return false;
          }
        }, TestHelper.WAIT_DURATION), "Instance capacity gauge metric is not found or incorrect!");
        Assert.assertEquals((long) mBeanServer.getAttribute(instanceObjectName, attributeName),
            (long) _capacityDataMap.get(capacityKey));
      }

      // Verify MaxCapacityUsageGauge
      Assert.assertTrue(TestHelper.verify(() -> {
        try {
          double actualMaxUsage = (double) mBeanServer.getAttribute(instanceObjectName,
              InstanceMonitor.InstanceMonitorMetric.MAX_CAPACITY_USAGE_GAUGE.metricName());
          // The values are manually calculated from the capacity configs, to make the code simple.
          double expectedMaxUsage = instance.equals(_testInstanceId) ? 0.4 : 0.0;

          return Math.abs(actualMaxUsage - expectedMaxUsage) < 0.000001d;
        } catch (AttributeNotFoundException e) {
          return false;
        }
      }, TestHelper.WAIT_DURATION), "MaxCapacityUsageGauge is not found or incorrect");
    }
  }

  @Override
  protected ResourceControllerDataProvider setupClusterDataCache() throws IOException {
    ResourceControllerDataProvider testCache = super.setupClusterDataCache();

    // Set up mock idealstate
    Map<String, IdealState> isMap = new HashMap<>();
    for (String resource : _resourceNames) {
      IdealState is = new IdealState(resource);
      is.setNumPartitions(_partitionNames.size());
      is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      is.setStateModelDefRef("MasterSlave");
      is.setReplicas("100");
      is.setRebalancerClassName(WagedRebalancer.class.getName());
      _partitionNames.stream()
          .forEach(partition -> is.setPreferenceList(partition, Collections.emptyList()));
      isMap.put(resource, is);
    }
    when(testCache.getIdealState(anyString())).thenAnswer(
        (Answer<IdealState>) invocationOnMock -> isMap.get(invocationOnMock.getArguments()[0]));
    when(testCache.getIdealStates()).thenReturn(isMap);
    when(testCache.getAsyncTasksThreadPool()).thenReturn(Executors.newSingleThreadExecutor());

    // Set up 2 more instances
    for (int i = 1; i < 3; i++) {
      String instanceName = _testInstanceId + i;
      _instances.add(instanceName);
      // 1. Set up the default instance information with capacity configuration.
      InstanceConfig testInstanceConfig = createMockInstanceConfig(instanceName);
      Map<String, InstanceConfig> instanceConfigMap = testCache.getInstanceConfigMap();
      instanceConfigMap.put(instanceName, testInstanceConfig);
      when(testCache.getInstanceConfigMap()).thenReturn(instanceConfigMap);
      // 2. Mock the live instance node for the default instance.
      LiveInstance testLiveInstance = createMockLiveInstance(instanceName);
      Map<String, LiveInstance> liveInstanceMap = testCache.getLiveInstances();
      liveInstanceMap.put(instanceName, testLiveInstance);
      when(testCache.getLiveInstances()).thenReturn(liveInstanceMap);
      when(testCache.getEnabledInstances()).thenReturn(liveInstanceMap.keySet());
      when(testCache.getEnabledLiveInstances()).thenReturn(liveInstanceMap.keySet());
      when(testCache.getAllInstances()).thenReturn(_instances);
    }

    return testCache;
  }
}
