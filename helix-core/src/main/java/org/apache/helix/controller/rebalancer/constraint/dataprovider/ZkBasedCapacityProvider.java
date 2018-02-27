package org.apache.helix.controller.rebalancer.constraint.dataprovider;

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

import org.apache.helix.*;
import org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import java.util.HashMap;
import java.util.Map;

/**
 * A capacity provider based on ZK node.
 * This class support persistent through Helix Property Store.
 */
public class ZkBasedCapacityProvider implements CapacityProvider {
  public static final int DEFAULT_CAPACITY_VALUE = 0;
  private static final String ROOT = "/PARTICIPANT_CAPACITY";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _dimensionPath;
  private ParticipantCapacity _capacity;

  /**
   * @param propertyStore The store that will be used to persist capacity information.
   * @param dimensionName Identify of the capacity attribute. For example memory, CPU.
   */
  public ZkBasedCapacityProvider(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String dimensionName) {
    _propertyStore = propertyStore;
    _dimensionPath = ROOT + "/" + dimensionName;

    ZNRecord existingRecord = _propertyStore.get(_dimensionPath, null, AccessOption.PERSISTENT);
    if (existingRecord == null) {
      // Create a capacity object using default capacity (DEFAULT_CAPACITY_VALUE).
      _capacity = new ParticipantCapacity(dimensionName);
    } else {
      _capacity = new ParticipantCapacity(existingRecord);
    }
  }

  /**
   * @param zkAddr
   * @param clusterName
   * @param dimensionName Identify of the capacity attribute. For example memory, CPU.
   *                      Need to match resource weight dimension.
   */
  public ZkBasedCapacityProvider(String zkAddr, String clusterName, String dimensionName) {
    this(new ZkHelixPropertyStore<ZNRecord>(zkAddr, new ZNRecordSerializer(),
        PropertyPathBuilder.propertyStore(clusterName)), dimensionName);
  }

  /**
   * Update capacity information.
   *
   * @param capacityMap     <ParticipantName, Total Participant Capacity>
   * @param usageMap        <ParticipantName, Usage>
   * @param defaultCapacity Default total capacity if not specified in the map
   */
  public void updateCapacity(Map<String, Integer> capacityMap, Map<String, Integer> usageMap,
      int defaultCapacity) {
    for (String participant : capacityMap.keySet()) {
      _capacity.setCapacity(participant, capacityMap.get(participant));
    }
    for (String participant : usageMap.keySet()) {
      _capacity.setUsage(participant, usageMap.get(participant));
    }
    _capacity.setDefaultCapacity(defaultCapacity);
  }

  /**
   * @return True if the capacity information is successfully wrote to ZK.
   */
  public boolean persistCapacity() {
    if (_capacity.isValid()) {
      return _propertyStore.set(_dimensionPath, _capacity.getRecord(), AccessOption.PERSISTENT);
    } else {
      throw new HelixException("Invalid ParticipantCapacity: " + _capacity.getRecord().toString());
    }
  }

  @Override
  public int getParticipantCapacity(String participant) {
    return _capacity.getCapacity(participant);
  }

  @Override
  public int getParticipantUsage(String participant) {
    return _capacity.getUsage(participant);
  }

  /**
   * Data model for participant capacity.
   * Per-participant capacity and usage are recorded in the mapfields.
   */
  private static class ParticipantCapacity extends HelixProperty {
    private static final String CAPACITY = "CAPACITY";
    private static final String USAGE = "USAGE_SIZE";

    enum ParticipantCapacityProperty {
      DEFAULT_CAPACITY,
    }

    ParticipantCapacity(String dimensionName) {
      super(dimensionName);
      _record
          .setIntField(ParticipantCapacityProperty.DEFAULT_CAPACITY.name(), DEFAULT_CAPACITY_VALUE);
    }

    ParticipantCapacity(ZNRecord record) {
      super(record);
      if (!isValid()) {
        throw new HelixException("Invalid ParticipantCapacity: " + record.toString());
      }
    }

    int getCapacity(String participant) {
      Map<String, String> participantMap = _record.getMapField(participant);
      if (participantMap != null && participantMap.containsKey(CAPACITY)) {
        return Integer.parseInt(participantMap.get(CAPACITY));
      }
      return getDefaultCapacity();
    }

    int getUsage(String participant) {
      Map<String, String> participantMap = _record.getMapField(participant);
      if (participantMap != null && participantMap.containsKey(USAGE)) {
        return Integer.parseInt(participantMap.get(USAGE));
      }
      return 0;
    }

    void setCapacity(String participant, int capacity) {
      Map<String, String> participantMap = getOrAddParticipantMap(participant);
      participantMap.put(CAPACITY, new Integer(capacity).toString());
    }

    void setUsage(String participant, int usage) {
      Map<String, String> participantMap = getOrAddParticipantMap(participant);
      participantMap.put(USAGE, new Integer(usage).toString());
    }

    private Map<String, String> getOrAddParticipantMap(String participant) {
      Map<String, String> participantMap = _record.getMapField(participant);
      if (participantMap == null) {
        participantMap = new HashMap<>();
        _record.setMapField(participant, participantMap);
      }
      return participantMap;
    }

    void setDefaultCapacity(int defaultCapacity) {
      _record.setIntField(ParticipantCapacityProperty.DEFAULT_CAPACITY.name(), defaultCapacity);
    }

    private int getDefaultCapacity() {
      return _record
          .getIntField(ParticipantCapacityProperty.DEFAULT_CAPACITY.name(), DEFAULT_CAPACITY_VALUE);
    }

    @Override
    public boolean isValid() {
      try {
        // check default capacity
        int defaultCapacity = getDefaultCapacity();
        if (defaultCapacity < 0) {
          return false;
        }
        // check if any invalid capacity values
        for (Map<String, String> capacityRecords : _record.getMapFields().values()) {
          if ((capacityRecords.containsKey(CAPACITY)
              && Integer.parseInt(capacityRecords.get(CAPACITY)) < 0) || (
              capacityRecords.containsKey(USAGE)
                  && Integer.parseInt(capacityRecords.get(USAGE)) < 0)) {
            return false;
          }
        }
        return true;
      } catch (Exception ex) {
        return false;
      }
    }
  }
}
