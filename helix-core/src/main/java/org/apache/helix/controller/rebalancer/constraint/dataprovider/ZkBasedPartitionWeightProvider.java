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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

/**
 * A resource weight provider based on ZK node.
 * This class support persistent through Helix Property Store.
 */
public class ZkBasedPartitionWeightProvider implements PartitionWeightProvider {
  public static final int DEFAULT_WEIGHT_VALUE = 1;
  private static final String ROOT = "/RESOURCE_WEIGHT";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _dimensionPath;
  private PartitionWeight _weights;

  /**
   * @param propertyStore The store that will be used to persist capacity information.
   * @param dimensionName Identify of the capacity attribute. For example memory, CPU.
   */
  public ZkBasedPartitionWeightProvider(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String dimensionName) {
    _propertyStore = propertyStore;
    _dimensionPath = ROOT + "/" + dimensionName;

    ZNRecord existingRecord = _propertyStore.get(_dimensionPath, null, AccessOption.PERSISTENT);
    if (existingRecord == null) {
      // Create default weight that return default weight only (DEFAULT_WEIGHT_VALUE).
      _weights = new PartitionWeight(dimensionName);
    } else {
      _weights = new PartitionWeight(existingRecord);
    }
  }

  /**
   * @param zkAddr
   * @param clusterName
   * @param dimensionName Identify of the capacity attribute. For example memory, CPU.
   *                      Need to match resource weight dimension.
   */
  public ZkBasedPartitionWeightProvider(String zkAddr, String clusterName, String dimensionName) {
    this(new ZkHelixPropertyStore<ZNRecord>(zkAddr, new ZNRecordSerializer(),
        PropertyPathBuilder.propertyStore(clusterName)), dimensionName);
  }

  /**
   * Update resources weight information.
   * @param resourceDefaultWeightMap <ResourceName, Resource Level Default Partition Weight>
   * @param partitionWeightMap <ResourceName, <PartitionName, Partition Weight>>
   * @param defaultWeight Overall Default partition Weight
   */
  public void updateWeights(Map<String, Integer> resourceDefaultWeightMap,
      Map<String, Map<String, Integer>> partitionWeightMap, int defaultWeight) {
    for (String resource : resourceDefaultWeightMap.keySet()) {
      _weights.setResourceDefaultWeight(resource, resourceDefaultWeightMap.get(resource));
    }
    for (String resource : partitionWeightMap.keySet()) {
      Map<String, Integer> detailMap = partitionWeightMap.get(resource);
      for (String partition : detailMap.keySet()) {
        _weights.setPartitionWeight(resource, partition, detailMap.get(partition));
      }
    }
    _weights.setDefaultWeight(defaultWeight);
  }

  /**
   * @return True if the weight information is successfully wrote to ZK.
   */
  public boolean persistWeights() {
    if (_weights.isValid()) {
      return _propertyStore.set(_dimensionPath, _weights.getRecord(), AccessOption.PERSISTENT);
    } else {
      throw new HelixException("Invalid ParticipantCapacity: " + _weights.getRecord().toString());
    }
  }

  @Override
  public int getPartitionWeight(String resource, String partition) {
    return _weights.getWeight(resource, partition);
  }

  /**
   * Data model for partition weight.
   * Default resource weight and overall default weight are stored in the simplefields.
   * Pre-partition weights are stored in the mapfields.
   */
  private static class PartitionWeight extends HelixProperty {
    enum ResourceWeightProperty {
      DEFAULT_WEIGHT,
      DEFAULT_RESOURCE_WEIGHT
    }

    PartitionWeight(String dimensionName) {
      super(dimensionName);
      _record.setIntField(ResourceWeightProperty.DEFAULT_WEIGHT.name(), DEFAULT_WEIGHT_VALUE);
    }

    PartitionWeight(ZNRecord record) {
      super(record);
      if (!isValid()) {
        throw new HelixException("Invalid ResourceWeight: " + record.toString());
      }
    }

    private String getWeightKey(String resource) {
      return ResourceWeightProperty.DEFAULT_RESOURCE_WEIGHT.name() + "_" + resource;
    }

    int getWeight(String resource, String partition) {
      Map<String, String> partitionWeightMap = _record.getMapField(resource);
      if (partitionWeightMap != null && partitionWeightMap.containsKey(partition)) {
        return Integer.parseInt(partitionWeightMap.get(partition));
      }
      return _record.getIntField(getWeightKey(resource), getDefaultWeight());
    }

    void setResourceDefaultWeight(String resource, int weight) {
      _record.setIntField(getWeightKey(resource), weight);
    }

    void setPartitionWeight(String resource, String partition, int weight) {
      Map<String, String> partitionWeightMap = _record.getMapField(resource);
      if (partitionWeightMap == null) {
        partitionWeightMap = new HashMap<>();
        _record.setMapField(resource, partitionWeightMap);
      }
      partitionWeightMap.put(partition, new Integer(weight).toString());
    }

    void setDefaultWeight(int defaultCapacity) {
      _record.setIntField(ResourceWeightProperty.DEFAULT_WEIGHT.name(), defaultCapacity);
    }

    private int getDefaultWeight() {
      return _record.getIntField(ResourceWeightProperty.DEFAULT_WEIGHT.name(), DEFAULT_WEIGHT_VALUE);
    }

    @Override
    public boolean isValid() {
      try {
        // check all default weights
        for (String weightStr : _record.getSimpleFields().values()) {
          if (Integer.parseInt(weightStr) < 0) {
            return false;
          }
        }
        // check all partition weights
        for (Map<String, String> partitionWeights : _record.getMapFields().values()) {
          for (String weightStr : partitionWeights.values()) {
            if (Integer.parseInt(weightStr) < 0) {
              return false;
            }
          }
        }
        return true;
      } catch (Exception ex) {
        return false;
      }
    }
  }
}
