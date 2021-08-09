package org.apache.helix.monitoring.mbeans;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.helix.TestHelper;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.assigner.TaskAssignResult;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.DefaultIdealStateCalculator;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

import static org.mockito.Mockito.when;


public class TestClusterStatusMonitor {
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();
  private String testDB = "TestDB";
  private String testDB_0 = testDB + "_0";

  @Test()
  public void testReportData() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ClusterStatusMonitor monitor = new ClusterStatusMonitor(clusterName);
    monitor.active();
    ObjectName clusterMonitorObjName = monitor.getObjectName(monitor.clusterBeanName());

    Assert.assertTrue(_server.isRegistered(clusterMonitorObjName));

    // Test #setPerInstanceResourceStatus()
    BestPossibleStateOutput bestPossibleStates = new BestPossibleStateOutput();
    bestPossibleStates.setState(testDB, new Partition(testDB_0), "localhost_12918", "MASTER");
    bestPossibleStates.setState(testDB, new Partition(testDB_0), "localhost_12919", "SLAVE");
    bestPossibleStates.setState(testDB, new Partition(testDB_0), "localhost_12920", "SLAVE");
    bestPossibleStates.setState(testDB, new Partition(testDB_0), "localhost_12921", "OFFLINE");
    bestPossibleStates.setState(testDB, new Partition(testDB_0), "localhost_12922", "DROPPED");

    Map<String, InstanceConfig> instanceConfigMap = Maps.newHashMap();
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      InstanceConfig config = new InstanceConfig(instanceName);
      instanceConfigMap.put(instanceName, config);
    }

    Map<String, Resource> resourceMap = Maps.newHashMap();
    Resource db = new Resource(testDB);
    db.setStateModelDefRef("MasterSlave");
    db.addPartition(testDB_0);
    resourceMap.put(testDB, db);

    Map<String, StateModelDefinition> stateModelDefMap = Maps.newHashMap();
    StateModelDefinition msStateModelDef =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    stateModelDefMap.put("MasterSlave", msStateModelDef);

    monitor.setPerInstanceResourceStatus(bestPossibleStates, instanceConfigMap, resourceMap,
        stateModelDefMap);

    // localhost_12918 should have 1 partition because it's MASTER
    ObjectName objName =
        monitor.getObjectName(monitor.getPerInstanceResourceBeanName("localhost_12918", testDB));
    Object value = _server.getAttribute(objName, "PartitionGauge");
    Assert.assertTrue(value instanceof Long);
    Assert.assertEquals((Long) value, new Long(1));
    value = _server.getAttribute(objName, "SensorName");
    Assert.assertTrue(value instanceof String);
    Assert.assertEquals((String) value, String.format("%s.%s.%s.%s.%s",
        ClusterStatusMonitor.PARTICIPANT_STATUS_KEY, clusterName, ClusterStatusMonitor.DEFAULT_TAG,
        "localhost_12918", testDB));

    // localhost_12919 should have 1 partition because it's SLAVE
    objName =
        monitor.getObjectName(monitor.getPerInstanceResourceBeanName("localhost_12919", testDB));
    value = _server.getAttribute(objName, "PartitionGauge");
    Assert.assertTrue(value instanceof Long);
    Assert.assertEquals((Long) value, new Long(1));

    // localhost_12921 should have 0 partition because it's OFFLINE
    objName =
        monitor.getObjectName(monitor.getPerInstanceResourceBeanName("localhost_12921", testDB));
    value = _server.getAttribute(objName, "PartitionGauge");
    Assert.assertTrue(value instanceof Long);
    Assert.assertEquals((Long) value, new Long(0));

    // localhost_12922 should have 0 partition because it's DROPPED
    objName =
        monitor.getObjectName(monitor.getPerInstanceResourceBeanName("localhost_12922", testDB));
    value = _server.getAttribute(objName, "PartitionGauge");
    Assert.assertTrue(value instanceof Long);
    Assert.assertEquals((Long) value, new Long(0));

    // Missing localhost_12918 in best possible ideal-state should remove it from mbean
    bestPossibleStates.getInstanceStateMap(testDB, new Partition(testDB_0)).remove(
        "localhost_12918");
    monitor.setPerInstanceResourceStatus(bestPossibleStates, instanceConfigMap, resourceMap,
        stateModelDefMap);

    objName =
        monitor.getObjectName(monitor.getPerInstanceResourceBeanName("localhost_12918", testDB));
    Assert.assertFalse(_server.isRegistered(objName),
        "Fail to unregister PerInstanceResource mbean for localhost_12918");

    // Clean up
    monitor.reset();

    objName =
        monitor.getObjectName(monitor.getPerInstanceResourceBeanName("localhost_12920", testDB));
    Assert.assertFalse(_server.isRegistered(objName),
        "Fail to unregister PerInstanceResource mbean for localhost_12920");

    Assert.assertFalse(_server.isRegistered(clusterMonitorObjName),
        "Failed to unregister ClusterStatusMonitor.");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testMessageMetrics() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ClusterStatusMonitor monitor = new ClusterStatusMonitor(clusterName);
    monitor.active();
    ObjectName clusterMonitorObjName = monitor.getObjectName(monitor.clusterBeanName());

    Assert.assertTrue(_server.isRegistered(clusterMonitorObjName));

    Map<String, Set<Message>> instanceMessageMap = Maps.newHashMap();
    Set<String> liveInstanceSet = Sets.newHashSet();
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      liveInstanceSet.add(instanceName);

      long now = System.currentTimeMillis();
      Set<Message> messages = Sets.newHashSet();
      // add 10 regular messages to each instance
      for (int j = 0; j < 10; j++) {
        Message m = new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
        m.setTgtName(instanceName);
        messages.add(m);
      }

      // add 10 past-due messages to each instance (using default completion period)
      for (int j = 0; j < 10; j++) {
        Message m = new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
        m.setTgtName(instanceName);
        m.setCreateTimeStamp(now - Message.MESSAGE_EXPECT_COMPLETION_PERIOD - 1000);
        messages.add(m);
      }

      // add other 5 past-due messages to each instance (using explicitly set COMPLETION time in message)
      for (int j = 0; j < 5; j++) {
        Message m = new Message(Message.MessageType.STATE_TRANSITION, UUID.randomUUID().toString());
        m.setTgtName(instanceName);
        m.setCompletionDueTimeStamp(now - 1000);
        messages.add(m);
      }
      instanceMessageMap.put(instanceName, messages);
    }

    monitor.setClusterInstanceStatus(liveInstanceSet, liveInstanceSet, Collections.emptySet(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), instanceMessageMap);

    Assert.assertEquals(monitor.getInstanceMessageQueueBacklog(), 25 * n);
    Assert.assertEquals(monitor.getTotalPastDueMessageGauge(), 15 * n);

    Object totalMsgSize =
        _server.getAttribute(clusterMonitorObjName, "InstanceMessageQueueBacklog");
    Assert.assertTrue(totalMsgSize instanceof Long);
    Assert.assertEquals((long) totalMsgSize, 25 * n);

    Object totalPastdueMsgCount =
        _server.getAttribute(clusterMonitorObjName, "TotalPastDueMessageGauge");
    Assert.assertTrue(totalPastdueMsgCount instanceof Long);
    Assert.assertEquals((long) totalPastdueMsgCount, 15 * n);

    for (String instance : liveInstanceSet) {
      ObjectName objName =
          monitor.getObjectName(monitor.getInstanceBeanName(instance));
      Object messageSize = _server.getAttribute(objName, "MessageQueueSizeGauge");
      Assert.assertTrue(messageSize instanceof Long);
      Assert.assertEquals((long) messageSize, 25L);

      Object pastdueMsgCount = _server.getAttribute(objName, "PastDueMessageGauge");
      Assert.assertTrue(pastdueMsgCount instanceof Long);
      Assert.assertEquals((long) pastdueMsgCount, 15L);
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testResourceAggregation() throws JMException, IOException {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ClusterStatusMonitor monitor = new ClusterStatusMonitor(clusterName);
    monitor.active();
    ObjectName clusterMonitorObjName = monitor.getObjectName(monitor.clusterBeanName());

    Assert.assertTrue(_server.isRegistered(clusterMonitorObjName));

    int numInstance = 5;
    int numPartition = 10;
    int numReplica = 3;
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < numInstance; i++) {
      String instance = "localhost_" + (12918 + i);
      instances.add(instance);
    }

    ZNRecord idealStateRecord = DefaultIdealStateCalculator
        .calculateIdealState(instances, numPartition, numReplica, testDB, "MASTER", "SLAVE");
    IdealState idealState = new IdealState(TestResourceMonitor.deepCopyZNRecord(idealStateRecord));
    idealState.setMinActiveReplicas(numReplica);
    ExternalView externalView = new ExternalView(TestResourceMonitor.deepCopyZNRecord(idealStateRecord));
    StateModelDefinition stateModelDef =
        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition();

    monitor.setResourceState(testDB, externalView, idealState, stateModelDef);

    Assert.assertEquals(monitor.getTotalPartitionGauge(), numPartition);
    Assert.assertEquals(monitor.getTotalResourceGauge(), 1);
    Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), 0);
    Assert.assertEquals(monitor.getStateTransitionCounter(), 0);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), 0);
    Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), 0);

    int lessMinActiveReplica = 6;
    Random r = new Random();
    externalView = new ExternalView(TestResourceMonitor.deepCopyZNRecord(idealStateRecord));
    int start = r.nextInt(numPartition - lessMinActiveReplica - 1);
    for (int i = start; i < start + lessMinActiveReplica; i++) {
      String partition = testDB + "_" + i;
      Map<String, String> map = externalView.getStateMap(partition);
      Iterator<String> it = map.keySet().iterator();
      int flag = 0;
      while (it.hasNext()) {
        String key = it.next();
        if (map.get(key).equalsIgnoreCase("SLAVE")) {
          if (flag++ % 2 == 0) {
            map.put(key, "OFFLINE");
          } else {
            it.remove();
          }
        }
      }
      externalView.setStateMap(partition, map);
    }

    monitor.setResourceState(testDB, externalView, idealState, stateModelDef);
    Assert.assertEquals(monitor.getTotalPartitionGauge(), numPartition);
    Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), lessMinActiveReplica);
    Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), lessMinActiveReplica);
    Assert.assertEquals(monitor.getStateTransitionCounter(), 0);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), 0);
    Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), lessMinActiveReplica);

    int missTopState = 7;
    externalView = new ExternalView(TestResourceMonitor.deepCopyZNRecord(idealStateRecord));
    start = r.nextInt(numPartition - missTopState - 1);
    for (int i = start; i < start + missTopState; i++) {
      String partition = testDB + "_" + i;
      Map<String, String> map = externalView.getStateMap(partition);
      int flag = 0;
      for (String key : map.keySet()) {
        if (map.get(key).equalsIgnoreCase("MASTER")) {
          if (flag++ % 2 == 0) {
            map.put(key, "OFFLINE");
          } else {
            map.remove(key);
          }
          break;
        }
      }
      externalView.setStateMap(partition, map);
    }

    monitor.setResourceState(testDB, externalView, idealState, stateModelDef);
    Assert.assertEquals(monitor.getTotalPartitionGauge(), numPartition);
    Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), missTopState);
    Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), missTopState);
    Assert.assertEquals(monitor.getStateTransitionCounter(), 0);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), 0);
    Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), missTopState);

    int missReplica = 5;
    externalView = new ExternalView(TestResourceMonitor.deepCopyZNRecord(idealStateRecord));
    start = r.nextInt(numPartition - missReplica - 1);
    for (int i = start; i < start + missReplica; i++) {
      String partition = testDB + "_" + i;
      Map<String, String> map = externalView.getStateMap(partition);
      Iterator<String> it = map.keySet().iterator();
      while (it.hasNext()) {
        String key = it.next();
        if (map.get(key).equalsIgnoreCase("SLAVE")) {
          it.remove();
          break;
        }
      }
      externalView.setStateMap(partition, map);
    }

    monitor.setResourceState(testDB, externalView, idealState, stateModelDef);
    Assert.assertEquals(monitor.getTotalPartitionGauge(), numPartition);
    Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), missReplica);
    Assert.assertEquals(monitor.getStateTransitionCounter(), 0);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), 0);
    Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), missReplica);

    int messageCount = 4;
    List<Message> messages = new ArrayList<>();
    for (int i = 0; i < messageCount; i++) {
      Message message = new Message(Message.MessageType.STATE_TRANSITION, "message" + i);
      message.setResourceName(testDB);
      message.setTgtName(instances.get(i % instances.size()));
      messages.add(message);
    }
    monitor.increaseMessageReceived(messages);
    Assert.assertEquals(monitor.getStateTransitionCounter(), messageCount);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), 0);

    // test pending state transition message report and read
    messageCount = new Random().nextInt(numPartition) + 1;
    monitor.setResourcePendingMessages(testDB, messageCount);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), messageCount);

    // Reset monitor.
    monitor.reset();
    Assert.assertFalse(_server.isRegistered(clusterMonitorObjName),
        "Failed to unregister ClusterStatusMonitor.");
  }

  @Test
  public void testUpdateInstanceCapacityStatus()
      throws MalformedObjectNameException, IOException, AttributeNotFoundException, MBeanException,
             ReflectionException, InstanceNotFoundException {
    String clusterName = "testCluster";
    List<Double> maxUsageList = ImmutableList.of(0.0d, 0.32d, 0.85d, 1.0d, 0.50d, 0.75d);
    Map<String, Double> maxUsageMap = new HashMap<>();
    Map<String, Map<String, Integer>> instanceCapacityMap = new HashMap<>();
    Random rand = new Random();

    for (int i = 0; i < maxUsageList.size(); i++) {
      String instanceName = "instance" + i;
      maxUsageMap.put(instanceName, maxUsageList.get(i));
      instanceCapacityMap.put(instanceName,
          ImmutableMap.of("capacity1", rand.nextInt(100), "capacity2", rand.nextInt(100)));
    }

    // Setup cluster status monitor.
    ClusterStatusMonitor monitor = new ClusterStatusMonitor(clusterName);
    monitor.active();
    ObjectName clusterMonitorObjName = monitor.getObjectName(monitor.clusterBeanName());

    // Cluster status monitor is registered.
    Assert.assertTrue(_server.isRegistered(clusterMonitorObjName));

    // Before calling setClusterInstanceStatus, instance monitors are not yet registered.
    for (Map.Entry<String, Double> entry : maxUsageMap.entrySet()) {
      String instance = entry.getKey();
      String instanceBeanName = String
          .format("%s,%s=%s", monitor.clusterBeanName(), ClusterStatusMonitor.INSTANCE_DN_KEY,
              instance);
      ObjectName instanceObjectName = monitor.getObjectName(instanceBeanName);

      Assert.assertFalse(_server.isRegistered(instanceObjectName));
    }

    // Call setClusterInstanceStatus to register instance monitors.
    monitor.setClusterInstanceStatus(maxUsageMap.keySet(), maxUsageMap.keySet(),
        Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap());

    // Update instance capacity status.
    for (Map.Entry<String, Double> usageEntry : maxUsageMap.entrySet()) {
      String instanceName = usageEntry.getKey();
      monitor.updateInstanceCapacityStatus(instanceName, usageEntry.getValue(),
          instanceCapacityMap.get(instanceName));
    }

    verifyCapacityMetrics(monitor, maxUsageMap, instanceCapacityMap);

    // Change capacity keys: "capacity2" -> "capacity3"
    for (String instanceName : instanceCapacityMap.keySet()) {
      instanceCapacityMap.put(instanceName,
          ImmutableMap.of("capacity1", rand.nextInt(100), "capacity3", rand.nextInt(100)));
    }

    // Update instance capacity status.
    for (Map.Entry<String, Double> usageEntry : maxUsageMap.entrySet()) {
      String instanceName = usageEntry.getKey();
      monitor.updateInstanceCapacityStatus(instanceName, usageEntry.getValue(),
          instanceCapacityMap.get(instanceName));
    }

    // "capacity2" metric should not exist in MBean server.
    String removedAttribute = "capacity2Gauge";
    for (Map.Entry<String, Map<String, Integer>> instanceEntry : instanceCapacityMap.entrySet()) {
      String instance = instanceEntry.getKey();
      String instanceBeanName = String
          .format("%s,%s=%s", monitor.clusterBeanName(), ClusterStatusMonitor.INSTANCE_DN_KEY,
              instance);
      ObjectName instanceObjectName = monitor.getObjectName(instanceBeanName);

      try {
        _server.getAttribute(instanceObjectName, removedAttribute);
        Assert.fail();
      } catch (AttributeNotFoundException ex) {
        // Expected AttributeNotFoundException because "capacity2Gauge" metric does not exist in
        // MBean server.
      }
    }

    verifyCapacityMetrics(monitor, maxUsageMap, instanceCapacityMap);

    // Reset monitor.
    monitor.reset();
    Assert.assertFalse(_server.isRegistered(clusterMonitorObjName),
        "Failed to unregister ClusterStatusMonitor.");
    for (String instance : maxUsageMap.keySet()) {
      String instanceBeanName =
          String.format("%s,%s=%s", monitor.clusterBeanName(), ClusterStatusMonitor.INSTANCE_DN_KEY, instance);
      ObjectName instanceObjectName = monitor.getObjectName(instanceBeanName);
      Assert.assertFalse(_server.isRegistered(instanceObjectName),
          "Failed to unregister instance monitor for instance: " + instance);
    }
  }

  @Test
  public void testRecordAvailableThreadsPerType() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    ClusterStatusMonitor monitor = new ClusterStatusMonitor(clusterName);
    monitor.active();
    ObjectName clusterMonitorObjName = monitor.getObjectName(monitor.clusterBeanName());
    Assert.assertTrue(_server.isRegistered(clusterMonitorObjName));

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    Map<String, LiveInstance> liveInstanceMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      String instanceName = "localhost_" + (12918 + i);
      LiveInstance liveInstance = new LiveInstance(instanceName);
      InstanceConfig instanceConfig = new InstanceConfig(instanceName);
      liveInstanceMap.put(instanceName, liveInstance);
      instanceConfigMap.put(instanceName, instanceConfig);
    }

    ClusterConfig clusterConfig = new ClusterConfig(clusterName);
    clusterConfig.resetTaskQuotaRatioMap();
    clusterConfig.setTaskQuotaRatio("type1", 30);
    clusterConfig.setTaskQuotaRatio("type2", 10);

    TaskDataCache taskDataCache = Mockito.mock(TaskDataCache.class);
    when(taskDataCache.getJobConfigMap()).thenReturn(Collections.emptyMap());

    AssignableInstanceManager assignableInstanceManager = new AssignableInstanceManager();
    assignableInstanceManager.buildAssignableInstances(clusterConfig, taskDataCache,
        liveInstanceMap, instanceConfigMap);

    monitor.updateAvailableThreadsPerJob(assignableInstanceManager.getGlobalCapacityMap());
    ObjectName type1ObjectName = monitor.getObjectName(monitor.getJobBeanName("type1"));
    ObjectName type2ObjectName = monitor.getObjectName(monitor.getJobBeanName("type2"));
    Assert.assertTrue(_server.isRegistered(type1ObjectName));
    Assert.assertEquals(_server.getAttribute(type1ObjectName, "AvailableThreadGauge"), 90L);
    Assert.assertTrue(_server.isRegistered(type2ObjectName));
    Assert.assertEquals(_server.getAttribute(type2ObjectName, "AvailableThreadGauge"), 30L);

    TaskAssignResult taskAssignResult = Mockito.mock(TaskAssignResult.class);
    when(taskAssignResult.getQuotaType()).thenReturn("type1");
    // Use non-existing instance to bypass the actual assignment, but still decrease thread counts
    assignableInstanceManager.assign("UnknownInstance", taskAssignResult);
    // Do it twice for type 1
    assignableInstanceManager.assign("UnknownInstance", taskAssignResult);
    when(taskAssignResult.getQuotaType()).thenReturn("type2");
    assignableInstanceManager.assign("UnknownInstance", taskAssignResult);

    monitor.updateAvailableThreadsPerJob(assignableInstanceManager.getGlobalCapacityMap());
    Assert.assertEquals(_server.getAttribute(type1ObjectName, "AvailableThreadGauge"), 88L);
    Assert.assertEquals(_server.getAttribute(type2ObjectName, "AvailableThreadGauge"), 29L);
  }

  private void verifyCapacityMetrics(ClusterStatusMonitor monitor, Map<String, Double> maxUsageMap,
      Map<String, Map<String, Integer>> instanceCapacityMap)
      throws MalformedObjectNameException, IOException, AttributeNotFoundException, MBeanException,
             ReflectionException, InstanceNotFoundException {
    // Verify results.
    for (Map.Entry<String, Map<String, Integer>> instanceEntry : instanceCapacityMap.entrySet()) {
      String instance = instanceEntry.getKey();
      Map<String, Integer> capacityMap = instanceEntry.getValue();
      String instanceBeanName = String
          .format("%s,%s=%s", monitor.clusterBeanName(), ClusterStatusMonitor.INSTANCE_DN_KEY,
              instance);
      ObjectName instanceObjectName = monitor.getObjectName(instanceBeanName);

      Assert.assertTrue(_server.isRegistered(instanceObjectName));
      Assert.assertEquals(_server.getAttribute(instanceObjectName,
          InstanceMonitor.InstanceMonitorMetric.MAX_CAPACITY_USAGE_GAUGE.metricName()),
          maxUsageMap.get(instance));

      for (Map.Entry<String, Integer> capacityEntry : capacityMap.entrySet()) {
        String capacityKey = capacityEntry.getKey();
        String attributeName = capacityKey + "Gauge";
        Assert.assertEquals((long) _server.getAttribute(instanceObjectName, attributeName),
            (long) instanceCapacityMap.get(instance).get(capacityKey));
      }
    }
  }

  private void verifyMessageMetrics(ClusterStatusMonitor monitor, Map<String, Double> maxUsageMap,
      Map<String, Map<String, Integer>> instanceCapacityMap)
      throws MalformedObjectNameException, IOException, AttributeNotFoundException, MBeanException,
             ReflectionException, InstanceNotFoundException {
    // Verify results.
    for (Map.Entry<String, Map<String, Integer>> instanceEntry : instanceCapacityMap.entrySet()) {
      String instance = instanceEntry.getKey();
      Map<String, Integer> capacityMap = instanceEntry.getValue();
      String instanceBeanName = String
          .format("%s,%s=%s", monitor.clusterBeanName(), ClusterStatusMonitor.INSTANCE_DN_KEY,
              instance);
      ObjectName instanceObjectName = monitor.getObjectName(instanceBeanName);

      Assert.assertTrue(_server.isRegistered(instanceObjectName));
      Assert.assertEquals(_server.getAttribute(instanceObjectName,
          InstanceMonitor.InstanceMonitorMetric.MAX_CAPACITY_USAGE_GAUGE.metricName()),
          maxUsageMap.get(instance));

      for (Map.Entry<String, Integer> capacityEntry : capacityMap.entrySet()) {
        String capacityKey = capacityEntry.getKey();
        String attributeName = capacityKey + "Gauge";
        Assert.assertEquals((long) _server.getAttribute(instanceObjectName, attributeName),
            (long) instanceCapacityMap.get(instance).get(capacityKey));
      }
    }
  }
}
