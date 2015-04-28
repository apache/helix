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

import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Map;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.apache.helix.TestHelper;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.AutoModeISBuilder;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class TestClusterStatusMonitor {
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();

  @Test()
  public void testReportData() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 5;
    String testDB = "TestDB";
    String testDB_0 = testDB + "_0";

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ClusterStatusMonitor monitor = new ClusterStatusMonitor(clusterName);
    ObjectName clusterMonitorObjName = monitor.getObjectName(monitor.clusterBeanName());
    try {
      _server.getMBeanInfo(clusterMonitorObjName);
    } catch (Exception e) {
      Assert.fail("Fail to register ClusterStatusMonitor");
    }

    // Test #setPerInstanceResourceStatus()
    BestPossibleStateOutput bestPossibleStates = new BestPossibleStateOutput();
    ResourceAssignment assignment = new ResourceAssignment(ResourceId.from(testDB));
    Map<ParticipantId, State> replicaMap = Maps.newHashMap();
    replicaMap.put(ParticipantId.from("localhost_12918"), State.from("MASTER"));
    replicaMap.put(ParticipantId.from("localhost_12919"), State.from("SLAVE"));
    replicaMap.put(ParticipantId.from("localhost_12920"), State.from("SLAVE"));
    replicaMap.put(ParticipantId.from("localhost_12921"), State.from("OFFLINE"));
    replicaMap.put(ParticipantId.from("localhost_12922"), State.from("DROPPED"));
    assignment.addReplicaMap(PartitionId.from(testDB_0), replicaMap);
    bestPossibleStates.setResourceAssignment(ResourceId.from(testDB), assignment);

    Map<String, InstanceConfig> instanceConfigMap = Maps.newHashMap();
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      InstanceConfig config = new InstanceConfig(instanceName);
      instanceConfigMap.put(instanceName, config);
    }

    Map<ResourceId, ResourceConfig> resourceMap = Maps.newHashMap();
    ResourceId resourceId = ResourceId.from(testDB);
    AutoModeISBuilder idealStateBuilder = new AutoModeISBuilder(resourceId).add(testDB_0);
    idealStateBuilder.setStateModel("MasterSlave");
    IdealState idealState = idealStateBuilder.build();
    ResourceConfig resourceConfig =
        new ResourceConfig.Builder(resourceId).idealState(idealState).build();
    resourceMap.put(resourceId, resourceConfig);

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
    replicaMap.remove(ParticipantId.from("localhost_12918"));
    assignment.addReplicaMap(PartitionId.from(testDB_0), replicaMap);
    bestPossibleStates.setResourceAssignment(ResourceId.from(testDB), assignment);
    monitor.setPerInstanceResourceStatus(bestPossibleStates, instanceConfigMap, resourceMap,
        stateModelDefMap);
    try {
      objName =
          monitor.getObjectName(monitor.getPerInstanceResourceBeanName("localhost_12918", testDB));
      _server.getMBeanInfo(objName);
      Assert.fail("Fail to unregister PerInstanceResource mbean for localhost_12918");

    } catch (InstanceNotFoundException e) {
      // OK
    }

    // Clean up
    monitor.reset();

    try {
      objName =
          monitor.getObjectName(monitor.getPerInstanceResourceBeanName("localhost_12920", testDB));
      _server.getMBeanInfo(objName);
      Assert.fail("Fail to unregister PerInstanceResource mbean for localhost_12920");

    } catch (InstanceNotFoundException e) {
      // OK
    }

    try {
      _server.getMBeanInfo(clusterMonitorObjName);
      Assert.fail("Fail to unregister ClusterStatusMonitor");
    } catch (InstanceNotFoundException e) {
      // OK
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
