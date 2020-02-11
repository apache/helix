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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.DefaultIdealStateCalculator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResourceMonitor {
  String _clusterName = "Test-cluster";
  String _dbName = "TestDB";
  int _replicas = 3;
  int _partitions = 50;

  @Test()
  public void testReportData() throws JMException {
    final int n = 5;
    ResourceMonitor monitor =
        new ResourceMonitor(_clusterName, _dbName, new ObjectName("testDomain:key=value"));
    monitor.register();

    try {
      List<String> instances = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        String instance = "localhost_" + (12918 + i);
        instances.add(instance);
      }

      ZNRecord idealStateRecord = DefaultIdealStateCalculator
          .calculateIdealState(instances, _partitions, _replicas - 1, _dbName, "MASTER", "SLAVE");
      IdealState idealState = new IdealState(deepCopyZNRecord(idealStateRecord));
      idealState.setMinActiveReplicas(_replicas - 1);
      ExternalView externalView = new ExternalView(deepCopyZNRecord(idealStateRecord));
      StateModelDefinition stateModelDef =
          BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition();

      monitor.updateResourceState(externalView, idealState, stateModelDef);

      Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), 0);
      Assert.assertEquals(monitor.getErrorPartitionGauge(), 0);
      Assert.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
      Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), 0);
      Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);
      Assert.assertEquals(monitor.getBeanName(), _clusterName + " " + _dbName);

      int errorCount = 5;
      Random r = new Random();
      int start = r.nextInt(_partitions - errorCount - 1);
      for (int i = start; i < start + errorCount; i++) {
        String partition = _dbName + "_" + i;
        Map<String, String> map = externalView.getStateMap(partition);
        for (String key : map.keySet()) {
          if (map.get(key).equalsIgnoreCase("SLAVE")) {
            map.put(key, "ERROR");
            break;
          }
        }
        externalView.setStateMap(partition, map);
      }

      monitor.updateResourceState(externalView, idealState, stateModelDef);

      Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), errorCount);
      Assert.assertEquals(monitor.getErrorPartitionGauge(), errorCount);
      Assert.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
      Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), errorCount);
      Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);

      int lessMinActiveReplica = 6;
      externalView = new ExternalView(deepCopyZNRecord(idealStateRecord));
      start = r.nextInt(_partitions - lessMinActiveReplica - 1);
      for (int i = start; i < start + lessMinActiveReplica; i++) {
        String partition = _dbName + "_" + i;
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

      monitor.updateResourceState(externalView, idealState, stateModelDef);

      Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), lessMinActiveReplica);
      Assert.assertEquals(monitor.getErrorPartitionGauge(), 0);
      Assert.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), lessMinActiveReplica);
      Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), lessMinActiveReplica);
      Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);

      int lessReplica = 4;
      externalView = new ExternalView(deepCopyZNRecord(idealStateRecord));
      start = r.nextInt(_partitions - lessReplica - 1);
      for (int i = start; i < start + lessReplica; i++) {
        String partition = _dbName + "_" + i;
        Map<String, String> map = externalView.getStateMap(partition);
        int flag = 0;
        Iterator<String> it = map.keySet().iterator();
        while (it.hasNext()) {
          String key = it.next();
          if (map.get(key).equalsIgnoreCase("SLAVE")) {
            if (flag++ % 2 == 0) {
              map.put(key, "OFFLINE");
            } else {
              it.remove();
            }
            break;
          }
        }
        externalView.setStateMap(partition, map);
      }

      monitor.updateResourceState(externalView, idealState, stateModelDef);

      Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), lessReplica);
      Assert.assertEquals(monitor.getErrorPartitionGauge(), 0);
      Assert.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
      Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), lessReplica);
      Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), 0);

      int missTopState = 7;
      externalView = new ExternalView(deepCopyZNRecord(idealStateRecord));
      start = r.nextInt(_partitions - missTopState - 1);
      for (int i = start; i < start + missTopState; i++) {
        String partition = _dbName + "_" + i;
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

      monitor.updateResourceState(externalView, idealState, stateModelDef);

      Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), missTopState);
      Assert.assertEquals(monitor.getErrorPartitionGauge(), 0);
      Assert.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getPartitionGauge(), _partitions);
      Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
      Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), missTopState);
      Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), missTopState);

      Assert.assertEquals(monitor.getNumPendingStateTransitionGauge(), 0);
      // test pending state transition message report and read
      int messageCount = new Random().nextInt(_partitions) + 1;
      monitor.updatePendingStateTransitionMessages(messageCount);
      Assert.assertEquals(monitor.getNumPendingStateTransitionGauge(), messageCount);

      Assert.assertEquals(monitor.getRebalanceState(),
          ResourceMonitor.RebalanceStatus.UNKNOWN.name());
      monitor.setRebalanceState(ResourceMonitor.RebalanceStatus.NORMAL);
      Assert
          .assertEquals(monitor.getRebalanceState(), ResourceMonitor.RebalanceStatus.NORMAL.name());
      monitor.setRebalanceState(ResourceMonitor.RebalanceStatus.BEST_POSSIBLE_STATE_CAL_FAILED);
      Assert.assertEquals(monitor.getRebalanceState(),
          ResourceMonitor.RebalanceStatus.BEST_POSSIBLE_STATE_CAL_FAILED.name());
      monitor.setRebalanceState(ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED);
      Assert.assertEquals(monitor.getRebalanceState(),
          ResourceMonitor.RebalanceStatus.INTERMEDIATE_STATE_CAL_FAILED.name());
    } finally {
      // Has to unregister this monitor to clean up. Otherwise, later tests may be affected and fail.
      monitor.unregister();
    }
  }

  @Test
  public void testUpdatePartitionWeightStats() throws JMException, IOException {
    final MBeanServerConnection mBeanServer = ManagementFactory.getPlatformMBeanServer();
    final String clusterName = TestHelper.getTestMethodName();
    final String resource = "testDB";
    final ObjectName resourceObjectName = new ObjectName("testDomain:key=value");
    final ResourceMonitor monitor =
        new ResourceMonitor(clusterName, resource, resourceObjectName);
    monitor.register();

    try {
      Map<String, Map<String, Integer>> partitionWeightMap =
          ImmutableMap.of(resource, ImmutableMap.of("capacity1", 20, "capacity2", 40));

      // Update Metrics
      partitionWeightMap.values().forEach(monitor::updatePartitionWeightStats);

      verifyPartitionWeightMetrics(mBeanServer, resourceObjectName, partitionWeightMap);

      // Change capacity keys: "capacity2" -> "capacity3"
      partitionWeightMap =
          ImmutableMap.of(resource, ImmutableMap.of("capacity1", 20, "capacity3", 60));

      // Update metrics.
      partitionWeightMap.values().forEach(monitor::updatePartitionWeightStats);

      // Verify results.
      verifyPartitionWeightMetrics(mBeanServer, resourceObjectName, partitionWeightMap);

      // "capacity2" metric should not exist in MBean server.
      String removedAttribute = "capacity2Gauge";
      try {
        mBeanServer.getAttribute(resourceObjectName, removedAttribute);
        Assert.fail("AttributeNotFoundException should be thrown because attribute [capacity2Gauge]"
            + " is removed.");
      } catch (AttributeNotFoundException expected) {
      }
    } finally {
      // Reset monitor.
      monitor.unregister();
      Assert.assertFalse(mBeanServer.isRegistered(resourceObjectName),
          "Failed to unregister resource monitor.");
    }
  }

  /**
   * Return a deep copy of a ZNRecord.
   *
   * @return
   */
  public static ZNRecord deepCopyZNRecord(ZNRecord record) {
    ZNRecord copy = new ZNRecord(record.getId());

    copy.getSimpleFields().putAll(record.getSimpleFields());
    for (String mapKey : record.getMapFields().keySet()) {
      Map<String, String> mapField = record.getMapFields().get(mapKey);
      copy.getMapFields().put(mapKey, new TreeMap<>(mapField));
    }

    for (String listKey : record.getListFields().keySet()) {
      copy.getListFields().put(listKey, new ArrayList<>(record.getListFields().get(listKey)));
    }
    if (record.getRawPayload() != null) {
      byte[] rawPayload = new byte[record.getRawPayload().length];
      System.arraycopy(record.getRawPayload(), 0, rawPayload, 0, record.getRawPayload().length);
      copy.setRawPayload(rawPayload);
    }

    copy.setVersion(record.getVersion());
    copy.setCreationTime(record.getCreationTime());
    copy.setModifiedTime(record.getModifiedTime());
    copy.setEphemeralOwner(record.getEphemeralOwner());

    return copy;
  }

  private void verifyPartitionWeightMetrics(MBeanServerConnection mBeanServer,
      ObjectName objectName, Map<String, Map<String, Integer>> expectedPartitionWeightMap)
      throws IOException, AttributeNotFoundException, MBeanException, ReflectionException,
             InstanceNotFoundException {
    final String gaugeMetricSuffix = "Gauge";
    for (Map.Entry<String, Map<String, Integer>> entry : expectedPartitionWeightMap.entrySet()) {
      // Resource monitor for this resource is already registered.
      Assert.assertTrue(mBeanServer.isRegistered(objectName));

      for (Map.Entry<String, Integer> capacityEntry : entry.getValue().entrySet()) {
        String attributeName = capacityEntry.getKey() + gaugeMetricSuffix;
        try {
          // Wait until the attribute is already registered to mbean server.
          Assert.assertTrue(TestHelper.verify(
              () -> !mBeanServer.getAttributes(objectName, new String[]{attributeName}).isEmpty(),
              2000));
        } catch (Exception ignored) {
        }
        Assert.assertEquals((long) mBeanServer.getAttribute(objectName, attributeName),
            (long) capacityEntry.getValue());
      }
    }
  }
}
