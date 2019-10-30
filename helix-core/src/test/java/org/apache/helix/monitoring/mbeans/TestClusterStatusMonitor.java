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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
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
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestClusterStatusMonitor {
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();
  private String testDB = "TestDB";
  private String testDB_0 = testDB + "_0";

  @Test()
  public void testReportData()
      throws Exception {
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


  @Test
  public void testResourceAggregation()
      throws JMException, IOException {
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

    monitor.setResourceStatus(externalView, idealState, stateModelDef, 0);

    Assert.assertEquals(monitor.getTotalPartitionGauge(), numPartition);
    Assert.assertEquals(monitor.getTotalResourceGauge(), 1);
    Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), 0);
    Assert.assertEquals(monitor.getStateTransitionCounter(), 0);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), 0);


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

    monitor.setResourceStatus(externalView, idealState, stateModelDef, 0);
    Assert.assertEquals(monitor.getTotalPartitionGauge(), numPartition);
    Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), lessMinActiveReplica);
    Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), lessMinActiveReplica);
    Assert.assertEquals(monitor.getStateTransitionCounter(), 0);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), 0);

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

    monitor.setResourceStatus(externalView, idealState, stateModelDef, 0);
    Assert.assertEquals(monitor.getTotalPartitionGauge(), numPartition);
    Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), missTopState);
    Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), missTopState);
    Assert.assertEquals(monitor.getStateTransitionCounter(), 0);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), 0);

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

    monitor.setResourceStatus(externalView, idealState, stateModelDef, 0);
    Assert.assertEquals(monitor.getTotalPartitionGauge(), numPartition);
    Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), missReplica);
    Assert.assertEquals(monitor.getStateTransitionCounter(), 0);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), 0);

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
    monitor.setResourceStatus(externalView, idealState, stateModelDef, messageCount);
    Assert.assertEquals(monitor.getPendingStateTransitionGuage(), messageCount);

    // Reset monitor.
    monitor.reset();
    Assert.assertFalse(_server.isRegistered(clusterMonitorObjName),
        "Failed to unregister ClusterStatusMonitor.");
  }

  @Test
  public void testUpdateMaxCapacityUsage()
      throws MalformedObjectNameException, IOException, AttributeNotFoundException, MBeanException,
             ReflectionException, InstanceNotFoundException {
    String clusterName = "testCluster";
    List<Double> maxUsageList = ImmutableList.of(0.0d, 0.32d, 0.85d, 1.0d, 0.50d, 0.75d);
    Map<String, Double> maxCapacityUsageMap = new HashMap<>();
    for (int i = 0; i < maxUsageList.size(); i++) {
      maxCapacityUsageMap.put("instance" + i, maxUsageList.get(i));
    }

    // Setup cluster status monitor.
    ClusterStatusMonitor monitor = new ClusterStatusMonitor(clusterName);
    monitor.active();
    ObjectName clusterMonitorObjName = monitor.getObjectName(monitor.clusterBeanName());

    Assert.assertTrue(_server.isRegistered(clusterMonitorObjName));

    // Update stats.
    monitor.updateMaxCapacityUsage(maxCapacityUsageMap);

    // Verify results.
    for (Map.Entry<String, Double> entry : maxCapacityUsageMap.entrySet()) {
      String instance = entry.getKey();
      double usage = entry.getValue();
      String instanceBeanName =
          String.format("%s,%s=%s", monitor.clusterBeanName(), monitor.INSTANCE_DN_KEY, instance);
      ObjectName instanceObjectName = monitor.getObjectName(instanceBeanName);

      Assert.assertTrue(_server.isRegistered(instanceObjectName));
      Assert.assertEquals(_server.getAttribute(instanceObjectName, "MaxCapacityUsageGauge"), usage);
    }

    // Reset monitor.
    monitor.reset();
    Assert.assertFalse(_server.isRegistered(clusterMonitorObjName),
        "Failed to unregister ClusterStatusMonitor.");
    for (String instance : maxCapacityUsageMap.keySet()) {
      String instanceBeanName =
          String.format("%s,%s=%s", monitor.clusterBeanName(), monitor.INSTANCE_DN_KEY, instance);
      ObjectName instanceObjectName = monitor.getObjectName(instanceBeanName);
      Assert.assertFalse(_server.isRegistered(instanceObjectName),
          "Failed to unregister instance monitor for instance: " + instance);
    }
  }
}
