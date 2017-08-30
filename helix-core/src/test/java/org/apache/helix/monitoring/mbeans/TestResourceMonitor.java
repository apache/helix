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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.Random;
import java.util.TreeMap;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.helix.ZNRecord;
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

  @Test() public void testReportData() throws JMException {
    final int n = 5;
    ResourceMonitor monitor = new ResourceMonitor(_clusterName, _dbName, new ObjectName("testDomain:key=value"));

    List<String> instances = new ArrayList<String>();
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

    monitor.updateResource(externalView, idealState, stateModelDef);

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

    monitor.updateResource(externalView, idealState, stateModelDef);

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

    monitor.updateResource(externalView, idealState, stateModelDef);

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

    monitor.updateResource(externalView, idealState, stateModelDef);

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

    monitor.updateResource(externalView, idealState, stateModelDef);

    Assert.assertEquals(monitor.getDifferenceWithIdealStateGauge(), missTopState);
    Assert.assertEquals(monitor.getErrorPartitionGauge(), 0);
    Assert.assertEquals(monitor.getExternalViewPartitionGauge(), _partitions);
    Assert.assertEquals(monitor.getPartitionGauge(), _partitions);
    Assert.assertEquals(monitor.getMissingMinActiveReplicaPartitionGauge(), 0);
    Assert.assertEquals(monitor.getMissingReplicaPartitionGauge(), missTopState);
    Assert.assertEquals(monitor.getMissingTopStatePartitionGauge(), missTopState);
  }

  /**
   * Return a deep copy of a ZNRecord.
   *
   * @return
   */
  public ZNRecord deepCopyZNRecord(ZNRecord record) {
    ZNRecord copy = new ZNRecord(record.getId());

    copy.getSimpleFields().putAll(record.getSimpleFields());
    for (String mapKey : record.getMapFields().keySet()) {
      Map<String, String> mapField = record.getMapFields().get(mapKey);
      copy.getMapFields().put(mapKey, new TreeMap<String, String>(mapField));
    }

    for (String listKey : record.getListFields().keySet()) {
      copy.getListFields().put(listKey, new ArrayList<String>(record.getListFields().get(listKey)));
    }
    if (record.getRawPayload() != null) {
      byte[] rawPayload = new byte[record.getRawPayload().length];
      System.arraycopy(record.getRawPayload(), 0, rawPayload, 0, record.getRawPayload().length);
      copy.setRawPayload(rawPayload);
    }

    copy.setVersion(record.getVersion());
    copy.setCreationTime(record.getCreationTime());
    copy.setModifiedTime(record.getModifiedTime());

    return copy;
  }
}
