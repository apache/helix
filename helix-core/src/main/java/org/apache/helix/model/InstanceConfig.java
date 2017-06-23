package org.apache.helix.model;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

/**
 * Instance configurations
 */
public class InstanceConfig extends HelixProperty {
  /**
   * Configurable characteristics of an instance
   */
  public enum InstanceConfigProperty {
    HELIX_HOST,
    HELIX_PORT,
    HELIX_ZONE_ID,
    HELIX_ENABLED,
    HELIX_DISABLED_PARTITION,
    TAG_LIST,
    INSTANCE_WEIGHT,
    DOMAIN,
    MAX_CONCURRENT_TASK
  }
  public static final int WEIGHT_NOT_SET = -1;
  public static final int MAX_CONCURRENT_TASK_NOT_SET = -1;

  private static final Logger _logger = Logger.getLogger(InstanceConfig.class.getName());

  /**
   * Instantiate for a specific instance
   * @param instanceId the instance identifier
   */
  public InstanceConfig(String instanceId) {
    super(instanceId);
  }

  /**
   * Instantiate with a pre-populated record
   * @param record a ZNRecord corresponding to an instance configuration
   */
  public InstanceConfig(ZNRecord record) {
    super(record);
  }

  /**
   * Get the host name of the instance
   * @return the host name
   */
  public String getHostName() {
    return _record.getSimpleField(InstanceConfigProperty.HELIX_HOST.toString());
  }

  /**
   * Set the host name of the instance
   * @param hostName the host name
   */
  public void setHostName(String hostName) {
    _record.setSimpleField(InstanceConfigProperty.HELIX_HOST.toString(), hostName);
  }

  /**
   * Get the port that the instance can be reached at
   * @return the port
   */
  public String getPort() {
    return _record.getSimpleField(InstanceConfigProperty.HELIX_PORT.toString());
  }

  /**
   * Set the port that the instance can be reached at
   * @param port the port
   */
  public void setPort(String port) {
    _record.setSimpleField(InstanceConfigProperty.HELIX_PORT.toString(), port);
  }

  /**
   * Set the zone identifier for this instance.
   * This is deprecated, please use domain to set hierarchy tag for an instance.
   * @return
   */
  @Deprecated
  public String getZoneId() {
    return _record.getSimpleField(InstanceConfigProperty.HELIX_ZONE_ID.name());
  }

  public void setZoneId(String zoneId) {
    _record.setSimpleField(InstanceConfigProperty.HELIX_ZONE_ID.name(), zoneId);
  }

  /**
   * Domain represents a hierarchy identifier for an instance.
   * @return
   */
  public String getDomain() {
    return _record.getSimpleField(InstanceConfigProperty.DOMAIN.name());
  }

  /**
   * Domain represents a hierarchy identifier for an instance.
   * Example:  "cluster=myCluster,zone=myZone1,rack=myRack,host=hostname,instance=instance001".
   * @return
   */
  public void setDomain(String domain) {
    _record.setSimpleField(InstanceConfigProperty.DOMAIN.name(), domain);
  }

  public int getWeight() {
    String w = _record.getSimpleField(InstanceConfigProperty.INSTANCE_WEIGHT.name());
    if (w != null) {
      try {
        int weight = Integer.valueOf(w);
        return weight;
      } catch (NumberFormatException e) {
      }
    }
    return WEIGHT_NOT_SET;
  }

  public void setWeight(int weight) {
    if (weight <= 0) {
      throw new IllegalArgumentException("Instance weight can not be equal or less than 0!");
    }
    _record.setSimpleField(InstanceConfigProperty.INSTANCE_WEIGHT.name(), String.valueOf(weight));
  }

  /**
   * Get arbitrary tags associated with the instance
   * @return a list of tags
   */
  public List<String> getTags() {
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.toString());
    if (tags == null) {
      tags = new ArrayList<String>(0);
    }
    return tags;
  }

  /**
   * Add a tag to this instance
   * @param tag an arbitrary property of the instance
   */
  public void addTag(String tag) {
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.toString());
    if (tags == null) {
      tags = new ArrayList<String>(0);
    }
    if (!tags.contains(tag)) {
      tags.add(tag);
    }
    getRecord().setListField(InstanceConfigProperty.TAG_LIST.toString(), tags);
  }

  /**
   * Remove a tag from this instance
   * @param tag a property of this instance
   */
  public void removeTag(String tag) {
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.toString());
    if (tags == null) {
      return;
    }
    if (tags.contains(tag)) {
      tags.remove(tag);
    }
  }

  /**
   * Check if an instance contains a tag
   * @param tag the tag to check
   * @return true if the instance contains the tag, false otherwise
   */
  public boolean containsTag(String tag) {
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.toString());
    if (tags == null) {
      return false;
    }
    return tags.contains(tag);
  }

  /**
   * Check if this instance is enabled and able to serve replicas
   * @return true if enabled, false if disabled
   */
  public boolean getInstanceEnabled() {
    return _record.getBooleanField(InstanceConfigProperty.HELIX_ENABLED.toString(), true);
  }

  /**
   * Set the enabled state of the instance
   * @param enabled true to enable, false to disable
   */
  public void setInstanceEnabled(boolean enabled) {
    _record.setBooleanField(InstanceConfigProperty.HELIX_ENABLED.toString(), enabled);
  }

  /**
   * Check if this instance is enabled for a given partition
   * This API is deprecated, and will be removed in next major release.
   *
   * @param partition the partition name to check
   * @return true if the instance is enabled for the partition, false otherwise
   */
  @Deprecated
  public boolean getInstanceEnabledForPartition(String partition) {
    boolean enabled = true;
    Map<String, String> disabledPartitionMap =
        _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    for (String resourceName : disabledPartitionMap.keySet()) {
      enabled &= getInstanceEnabledForPartition(resourceName, partition);
    }
    return enabled;
  }

  /**
   * Check if this instance is enabled for a given partition
   * @param partition the partition name to check
   * @return true if the instance is enabled for the partition, false otherwise
   */
  public boolean getInstanceEnabledForPartition(String resource, String partition) {
    // TODO: Remove this old partition list check once old get API removed.
    List<String> oldDisabledPartition =
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    Map<String, String> disabledPartitionsMap =
        _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    if ((disabledPartitionsMap != null && disabledPartitionsMap.containsKey(resource) && HelixUtil
        .deserializeByComma(disabledPartitionsMap.get(resource)).contains(partition))
        || oldDisabledPartition != null && oldDisabledPartition.contains(partition)) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Get the partitions disabled by this instance
   * This method will be deprecated since we persist disabled partitions
   * based on instance and resource. The result will not be accurate as we
   * union all the partitions disabled.
   *
   * @return a list of partition names
   */
  @Deprecated
  public List<String> getDisabledPartitions() {
    List<String> oldDisabled =
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    if (!_record.getMapFields().containsKey(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name())
        && oldDisabled == null) {
      return null;
    }

    Set<String> disabledPartitions = new HashSet<String>();
    if (oldDisabled != null) {
      disabledPartitions.addAll(oldDisabled);
    }

    for (String perResource : _record
        .getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name()).values()) {
      disabledPartitions.addAll(HelixUtil.deserializeByComma(perResource));
    }

    return new ArrayList<String>(disabledPartitions);
  }

  /**
   * Get the partitions disabled by resource on this instance
   * @param resourceName  The resource of disabled partitions
   * @return              A list of partition names if exists, otherwise will be null
   */
  public List<String> getDisabledPartitions(String resourceName) {
    // TODO: Remove this logic getting data from list field when getDisabledParition() removed.
    List<String> oldDisabled =
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    if ((!_record.getMapFields().containsKey(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name())
        || !_record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name())
        .containsKey(resourceName)) && oldDisabled == null) {
      return null;
    }

    Set<String> disabledPartitions = new HashSet<String>();
    if (oldDisabled != null) {
      disabledPartitions.addAll(oldDisabled);
    }

    disabledPartitions.addAll(HelixUtil.deserializeByComma(
        _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name())
            .get(resourceName)));

    return new ArrayList<String>(disabledPartitions);
  }

  /**
  * Get a map that mapping resource name to disabled partitions
  * @return A map of resource name mapping to disabled partitions. If no
  *         resource/partitions disabled, return an empty map.
  */
  public Map<String, List<String>> getDisabledPartitionsMap() {
    Map<String, String> disabledPartitionsRawMap =
        _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    if (disabledPartitionsRawMap == null) {
      return Collections.emptyMap();
    }

    Map<String, List<String>> disabledPartitionsMap = new HashMap<String, List<String>>();
    for (String resourceName : disabledPartitionsRawMap.keySet()) {
      disabledPartitionsMap.put(resourceName, getDisabledPartitions(resourceName));
    }

    return disabledPartitionsMap;
  }



  /**
   * Set the enabled state for a partition on this instance across all the resources
   *
   * @param partitionName the partition to set
   * @param enabled true to enable, false to disable
   */
  @Deprecated
  public void setInstanceEnabledForPartition(String partitionName, boolean enabled) {
    List<String> list =
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
    Set<String> disabledPartitions = new HashSet<String>();
    if (list != null) {
      disabledPartitions.addAll(list);
    }

    if (enabled) {
      disabledPartitions.remove(partitionName);
    } else {
      disabledPartitions.add(partitionName);
    }

    list = new ArrayList<String>(disabledPartitions);
    Collections.sort(list);
    _record.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(), list);
  }

  public void setInstanceEnabledForPartition(String resourceName, String partitionName,
      boolean enabled) {
    // Get old disabled partitions if exists
    // TODO: Remove this when getDisabledParition() removed.
    List<String> oldDisabledPartitions =
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());

    Map<String, String> currentDisabled =
        _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    Set<String> disabledPartitions = new HashSet<String>();

    if (currentDisabled != null && currentDisabled.containsKey(resourceName)) {
      disabledPartitions.addAll(HelixUtil.deserializeByComma(currentDisabled.get(resourceName)));
    }

    if (enabled) {
      disabledPartitions.remove(partitionName);
      if (oldDisabledPartitions != null && oldDisabledPartitions.contains(partitionName)) {
        oldDisabledPartitions.remove(partitionName);
      }
    } else {
      disabledPartitions.add(partitionName);
    }

    List<String> disabledPartitionList = new ArrayList<String>(disabledPartitions);
    Collections.sort(disabledPartitionList);
    if (currentDisabled == null) {
      currentDisabled = new HashMap<String, String>();
    }

    if (disabledPartitionList != null) {
      currentDisabled.put(resourceName, HelixUtil.serializeByComma(disabledPartitionList));
    }

    if (!currentDisabled.isEmpty()) {
      _record.setMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name(), currentDisabled);
    }

    if (oldDisabledPartitions != null && !oldDisabledPartitions.isEmpty()) {
      _record.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name(),
          oldDisabledPartitions);
    }
  }

  /**
   * Get maximum allowed running task count on this instance
   * @return the maximum task count
   */
  public int getMaxConcurrentTask() {
    return _record.getIntField(InstanceConfigProperty.MAX_CONCURRENT_TASK.toString(), MAX_CONCURRENT_TASK_NOT_SET);
  }

  public void setMaxConcurrentTask(int maxConcurrentTask) {
    _record.setIntField(InstanceConfigProperty.MAX_CONCURRENT_TASK.toString(), maxConcurrentTask);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof InstanceConfig) {
      InstanceConfig that = (InstanceConfig) obj;

      if (this.getId().equals(that.getId())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  /**
   * Get the name of this instance
   * @return the instance name
   */
  public String getInstanceName() {
    return _record.getId();
  }

  @Override
  public boolean isValid() {
    // HELIX-65: remove check for hostname/port existence
    return true;
  }
}
