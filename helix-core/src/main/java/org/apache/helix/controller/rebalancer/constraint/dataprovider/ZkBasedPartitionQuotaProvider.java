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
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionQuotaProvider;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import java.util.HashMap;
import java.util.Map;

/**
 * A resource quota provider based on ZK node.
 * This class support persistent through Helix Property Store.
 */
public class ZkBasedPartitionQuotaProvider implements PartitionQuotaProvider {
  public static final int DEFAULT_QUOTA_VALUE = 1;
  private static final String ROOT = "/RESOURCE_QUOTA";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _dimensionPath;
  private PartitionQuota _quotas;

  /**
   * @param propertyStore The store that will be used to persist capacity information.
   * @param dimensionName Identify of the capacity attribute. For example memory, CPU.
   */
  public ZkBasedPartitionQuotaProvider(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String dimensionName) {
    _propertyStore = propertyStore;
    _dimensionPath = ROOT + "/" + dimensionName;

    ZNRecord existingRecord = _propertyStore.get(_dimensionPath, null, AccessOption.PERSISTENT);
    if (existingRecord == null) {
      // Create default quota that return default quota only (DEFAULT_QUOTA_VALUE).
      _quotas = new PartitionQuota(dimensionName);
    } else {
      _quotas = new PartitionQuota(existingRecord);
    }
  }

  /**
   * @param zkAddr
   * @param clusterName
   * @param dimensionName Identify of the capacity attribute. For example memory, CPU.
   *                      Need to match resource quota dimension.
   */
  public ZkBasedPartitionQuotaProvider(String zkAddr, String clusterName, String dimensionName) {
    this(new ZkHelixPropertyStore<ZNRecord>(zkAddr, new ZNRecordSerializer(),
        PropertyPathBuilder.propertyStore(clusterName)), dimensionName);
  }

  /**
   * Update resources quota information.
   * @param resourceDefaultQuotaMap <ResourceName, Resource Level Default Partition Quota>
   * @param partitionQuotaMap <ResourceName, <PartitionName, Partition Quota>>
   * @param defaultQuota Overall Default partition Quota
   */
  public void updateQuotas(Map<String, Integer> resourceDefaultQuotaMap,
      Map<String, Map<String, Integer>> partitionQuotaMap, int defaultQuota) {
    for (String resource : resourceDefaultQuotaMap.keySet()) {
      _quotas.setResourceDefaultQuota(resource, resourceDefaultQuotaMap.get(resource));
    }
    for (String resource : partitionQuotaMap.keySet()) {
      Map<String, Integer> detailMap = partitionQuotaMap.get(resource);
      for (String partition : detailMap.keySet()) {
        _quotas.setPartitionQuota(resource, partition, detailMap.get(partition));
      }
    }
    _quotas.setDefaultQuota(defaultQuota);
  }

  /**
   * @return True if the quota information is successfully wrote to ZK.
   */
  public boolean persistQuotas() {
    if (_quotas.isValid()) {
      return _propertyStore.set(_dimensionPath, _quotas.getRecord(), AccessOption.PERSISTENT);
    } else {
      throw new HelixException("Invalid ParticipantCapacity: " + _quotas.getRecord().toString());
    }
  }

  @Override
  public int getPartitionQuota(String resource, String partition) {
    return _quotas.getQuota(resource, partition);
  }

  /**
   * Data model for partition quota.
   * Default resource quota and overall default quota are stored in the simplefields.
   * Pre-partition quotas are stored in the mapfields.
   */
  private static class PartitionQuota extends HelixProperty {
    enum ResourceQuotaProperty {
      DEFAULT_QUOTA,
      DEFAULT_RESOURCE_QUOTA
    }

    PartitionQuota(String dimensionName) {
      super(dimensionName);
      _record.setIntField(ResourceQuotaProperty.DEFAULT_QUOTA.name(), DEFAULT_QUOTA_VALUE);
    }

    PartitionQuota(ZNRecord record) {
      super(record);
      if (!isValid()) {
        throw new HelixException("Invalid ResourceQuota: " + record.toString());
      }
    }

    private String getQuotaKey(String resource) {
      return ResourceQuotaProperty.DEFAULT_RESOURCE_QUOTA.name() + "_" + resource;
    }

    int getQuota(String resource, String partition) {
      Map<String, String> partitionQuotaMap = _record.getMapField(resource);
      if (partitionQuotaMap != null && partitionQuotaMap.containsKey(partition)) {
        return Integer.parseInt(partitionQuotaMap.get(partition));
      }
      return _record.getIntField(getQuotaKey(resource), getDefaultQuota());
    }

    void setResourceDefaultQuota(String resource, int quota) {
      _record.setIntField(getQuotaKey(resource), quota);
    }

    void setPartitionQuota(String resource, String partition, int quota) {
      Map<String, String> partitionQuotaMap = _record.getMapField(resource);
      if (partitionQuotaMap == null) {
        partitionQuotaMap = new HashMap<>();
        _record.setMapField(resource, partitionQuotaMap);
      }
      partitionQuotaMap.put(partition, new Integer(quota).toString());
    }

    void setDefaultQuota(int defaultCapacity) {
      _record.setIntField(ResourceQuotaProperty.DEFAULT_QUOTA.name(), defaultCapacity);
    }

    private int getDefaultQuota() {
      return _record.getIntField(ResourceQuotaProperty.DEFAULT_QUOTA.name(), DEFAULT_QUOTA_VALUE);
    }

    @Override
    public boolean isValid() {
      try {
        // check all default quotas
        for (String quotaStr : _record.getSimpleFields().values()) {
          if (Integer.parseInt(quotaStr) < 0) {
            return false;
          }
        }
        // check all partition quotas
        for (Map<String, String> partitionQuotas : _record.getMapFields().values()) {
          for (String quotaStr : partitionQuotas.values()) {
            if (Integer.parseInt(quotaStr) < 0) {
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
