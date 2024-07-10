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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.util.ConfigStringUtil;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    @Deprecated HELIX_ENABLED,
    HELIX_ENABLED_TIMESTAMP,
    @Deprecated HELIX_DISABLED_REASON,
    @Deprecated HELIX_DISABLED_TYPE,
    HELIX_DISABLED_PARTITION,
    TAG_LIST,
    INSTANCE_WEIGHT,
    DOMAIN,
    DELAY_REBALANCE_ENABLED,
    MAX_CONCURRENT_TASK,
    INSTANCE_INFO_MAP,
    INSTANCE_CAPACITY_MAP, TARGET_TASK_THREAD_POOL_SIZE, HELIX_INSTANCE_OPERATIONS
  }

  public static class InstanceOperation {
    private final Map<String, String> _properties;

    private enum InstanceOperationProperties {
      OPERATION, REASON, SOURCE, TIMESTAMP
    }

    private InstanceOperation(@Nullable Map<String, String> properties) {
      // Default to ENABLE operation if no operation type is provided.
      _properties = properties == null ? new HashMap<>() : properties;
      if (!_properties.containsKey(InstanceOperationProperties.OPERATION.name())) {
        _properties.put(InstanceOperationProperties.OPERATION.name(),
            InstanceConstants.InstanceOperation.ENABLE.name());
      }
    }

    public static class Builder {
      private Map<String, String> _properties = new HashMap<>();

      /**
       * Set the operation type for this instance operation.
       * @param operationType InstanceOperation type of this instance operation.
       */
      public Builder setOperation(@Nullable InstanceConstants.InstanceOperation operationType) {
        _properties.put(InstanceOperationProperties.OPERATION.name(),
            operationType == null ? InstanceConstants.InstanceOperation.ENABLE.name()
                : operationType.name());
        return this;
      }

      /**
       * Set the reason for this instance operation.
       * @param reason
       */
      public Builder setReason(String reason) {
        _properties.put(InstanceOperationProperties.REASON.name(), reason != null ? reason : "");
        return this;
      }

      /**
       * Set the source for this instance operation.
       * @param source InstanceOperationSource
       *              that caused this instance operation to be triggered.
       */
      public Builder setSource(InstanceConstants.InstanceOperationSource source) {
        _properties.put(InstanceOperationProperties.SOURCE.name(),
            source == null ? InstanceConstants.InstanceOperationSource.USER.name()
                : source.name());
        return this;
      }

      public InstanceOperation build() throws IllegalArgumentException {
        if (!_properties.containsKey(InstanceOperationProperties.OPERATION.name())) {
          throw new IllegalArgumentException(
              "Instance operation type is not set, this is a required field.");
        }
        _properties.put(InstanceOperationProperties.TIMESTAMP.name(),
            String.valueOf(System.currentTimeMillis()));
        return new InstanceOperation(_properties);
      }
    }

    /**
     * Get the operation type of this instance operation.
     * @return the InstanceOperation type
     */
    public InstanceConstants.InstanceOperation getOperation() throws IllegalArgumentException {
      return InstanceConstants.InstanceOperation.valueOf(
          _properties.get(InstanceOperationProperties.OPERATION.name()));
    }

    /**
     * Get the reason for this instance operation.
     * If the reason is not set, it will default to an empty string.
     *
     * @return the reason for this instance operation.
     */
    public String getReason() {
      return _properties.getOrDefault(InstanceOperationProperties.REASON.name(), "");
    }

    /**
     * Get the InstanceOperationSource
     * that caused this instance operation to be triggered.
     * If the source is not set, it will default to DEFAULT.
     *
     * @return the InstanceOperationSource
     *that caused this instance operation to be triggered.
     */
    public InstanceConstants.InstanceOperationSource getSource() {
      return InstanceConstants.InstanceOperationSource.valueOf(
          _properties.getOrDefault(InstanceOperationProperties.SOURCE.name(),
              InstanceConstants.InstanceOperationSource.USER.name()));
    }

    /**
     * Get the timestamp (milliseconds from epoch) when this instance operation was triggered.
     *
     * @return the timestamp when the instance operation was triggered.
     */
    public long getTimestamp() {
      return Long.parseLong(_properties.get(InstanceOperationProperties.TIMESTAMP.name()));
    }

    private void setTimestamp(long timestamp) {
      _properties.put(InstanceOperationProperties.TIMESTAMP.name(), String.valueOf(timestamp));
    }

    private Map<String, String> getProperties() {
      return _properties;
    }
  }

  public static final int WEIGHT_NOT_SET = -1;
  public static final int MAX_CONCURRENT_TASK_NOT_SET = -1;
  private static final int TARGET_TASK_THREAD_POOL_SIZE_NOT_SET = -1;
  private static final boolean HELIX_ENABLED_DEFAULT_VALUE = true;
  private static final long HELIX_ENABLED_TIMESTAMP_DEFAULT_VALUE = -1;
  private static final ObjectMapper _objectMapper = new ObjectMapper();

  // These fields are not allowed to be overwritten by the merge method because
  // they are unique properties of an instance.
  private static final ImmutableSet<InstanceConfigProperty> NON_OVERWRITABLE_PROPERTIES =
      ImmutableSet.of(InstanceConfigProperty.HELIX_HOST, InstanceConfigProperty.HELIX_PORT,
          InstanceConfigProperty.HELIX_ZONE_ID, InstanceConfigProperty.DOMAIN,
          InstanceConfigProperty.INSTANCE_INFO_MAP);

  private static final Logger _logger = LoggerFactory.getLogger(InstanceConfig.class.getName());

  private List<InstanceOperation> _deserializedInstanceOperations;

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
    return _record.getSimpleField(InstanceConfigProperty.HELIX_HOST.name());
  }

  /**
   * Set the host name of the instance
   * @param hostName the host name
   */
  public void setHostName(String hostName) {
    _record.setSimpleField(InstanceConfigProperty.HELIX_HOST.name(), hostName);
  }

  /**
   * Get the port that the instance can be reached at
   * @return the port
   */
  public String getPort() {
    return _record.getSimpleField(InstanceConfigProperty.HELIX_PORT.name());
  }

  /**
   * Set the port that the instance can be reached at
   * @param port the port
   */
  public void setPort(String port) {
    _record.setSimpleField(InstanceConfigProperty.HELIX_PORT.name(), port);
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
   * This is to ensure backward compatibility, going forward please use {@link InstanceConfig#getDomainAsString()}
   * @return
   */
  @Deprecated
  public String getDomain() {
    return _record.getSimpleField(InstanceConfigProperty.DOMAIN.name());
  }

  /**
   * Domain represents a hierarchy identifier for an instance.
   * @return
   */
  public String getDomainAsString() {
    return _record.getSimpleField(InstanceConfigProperty.DOMAIN.name());
  }

  /**
   * Parse the key value pairs of domain and return a map structure
   * @return
   */
  public Map<String, String> getDomainAsMap() {
    String domain = getDomainAsString();
    return ConfigStringUtil.parseConcatenatedConfig(getDomainAsString());
  }

  /**
   * Domain represents a hierarchy identifier for an instance.
   * Example:  "cluster=myCluster,zone=myZone1,rack=myRack,host=hostname,instance=instance001".
   */
  public void setDomain(String domain) {
    _record.setSimpleField(InstanceConfigProperty.DOMAIN.name(), domain);
  }

  /**
   * Set domain from its map representation.
   * @param domainMap domain as a map
   */
  public void setDomain(Map<String, String> domainMap) {
    setDomain(ConfigStringUtil.concatenateMapping(domainMap));
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
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.name());
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
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.name());
    if (tags == null) {
      tags = new ArrayList<String>(0);
    }
    if (!tags.contains(tag)) {
      tags.add(tag);
    }
    getRecord().setListField(InstanceConfigProperty.TAG_LIST.name(), tags);
  }

  /**
   * Remove a tag from this instance
   * @param tag a property of this instance
   */
  public void removeTag(String tag) {
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.name());
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
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.name());
    if (tags == null) {
      return false;
    }
    return tags.contains(tag);
  }

  /**
   * Get the timestamp (milliseconds from epoch) when this instance was enabled/disabled last time.
   *
   * @return the timestamp when the instance was enabled/disabled last time. If the instance is never
   *        enabled/disabled, return -1.
   */
  public long getInstanceEnabledTime() {
    return _record.getLongField(InstanceConfigProperty.HELIX_ENABLED_TIMESTAMP.name(),
        HELIX_ENABLED_TIMESTAMP_DEFAULT_VALUE);
  }

  /**
   * Set the enabled state of the instance If user enables the instance, HELIX_DISABLED_REASON filed
   * will be removed.
   * @param enabled true to enable, false to disable
   * @deprecated This method is deprecated. Please use setInstanceOperation instead.
   */
  @Deprecated
  public void setInstanceEnabled(boolean enabled) {
    // set instance operation only when we need to change InstanceEnabled value.
    setInstanceEnabledHelper(enabled, null);
  }

  private void setInstanceEnabledHelper(boolean enabled, Long timestampOverride) {
    _record.setBooleanField(InstanceConfigProperty.HELIX_ENABLED.name(), enabled);
    _record.setLongField(InstanceConfigProperty.HELIX_ENABLED_TIMESTAMP.name(),
        timestampOverride != null ? timestampOverride : System.currentTimeMillis());
    if (enabled) {
      // TODO: Replace this when HELIX_ENABLED and HELIX_DISABLED_REASON is removed.
      resetInstanceDisabledTypeAndReason();
    }
  }

  /**
   * Removes HELIX_DISABLED_REASON and HELIX_DISABLED_TYPE entry from simple field.
   */
  @Deprecated
  public void resetInstanceDisabledTypeAndReason() {
    _record.getSimpleFields().remove(InstanceConfigProperty.HELIX_DISABLED_REASON.name());
    _record.getSimpleFields().remove(InstanceConfigProperty.HELIX_DISABLED_TYPE.name());
  }

  /**
   * Set the instance disabled reason when instance is disabled.
   * It will be a no-op when instance is enabled.
   * @deprecated This method is deprecated. Please use .
   */
  @Deprecated
  public void setInstanceDisabledReason(String disabledReason) {
    if (getInstanceOperation().getOperation().equals(InstanceConstants.InstanceOperation.DISABLE)) {
      _record.setSimpleField(InstanceConfigProperty.HELIX_DISABLED_REASON.name(), disabledReason);
    }
  }

  /**
   * Set the instance disabled type when instance is disabled.
   * It will be a no-op when instance is enabled.
   * @deprecated This method is deprecated. Please use setInstanceOperation along with
   * InstanceOperation.Builder().setSource
   *(...)
   */
  @Deprecated
  public void setInstanceDisabledType(InstanceConstants.InstanceDisabledType disabledType) {
    if (getInstanceOperation().getOperation().equals(InstanceConstants.InstanceOperation.DISABLE)) {
      _record.setSimpleField(InstanceConfigProperty.HELIX_DISABLED_TYPE.name(),
          disabledType.name());
    }
  }

  /**
   * Get the instance disabled reason when instance is disabled.
   * @return Return instance disabled reason. Default is am empty string.
   * @deprecated This method is deprecated. Please use getInstanceOperation().getReason() instead.
   */
  @Deprecated
  public String getInstanceDisabledReason() {
    return _record.getStringField(InstanceConfigProperty.HELIX_DISABLED_REASON.name(), "");
  }

  /**
   *
   * @return Return instance disabled type (org.apache.helix.constants.InstanceConstants.InstanceDisabledType)
   *         Default is am empty string.
   * @deprecated This method is deprecated. Please use getInstanceOperation().getSource
   *() instead.
   */
  @Deprecated
  public String getInstanceDisabledType() {
    if (_record.getBooleanField(InstanceConfigProperty.HELIX_ENABLED.name(),
        HELIX_ENABLED_DEFAULT_VALUE)) {
      return InstanceConstants.INSTANCE_NOT_DISABLED;
    }
    return _record.getStringField(InstanceConfigProperty.HELIX_DISABLED_TYPE.name(),
        InstanceConstants.InstanceDisabledType.DEFAULT_INSTANCE_DISABLE_TYPE.name());
  }

  private List<InstanceOperation> getInstanceOperations() {
    if (_deserializedInstanceOperations == null || _deserializedInstanceOperations.isEmpty()) {
      // If the _deserializedInstanceOperations is not set, then we need to build it from the real
      // helix property HELIX_INSTANCE_OPERATIONS.
      List<String> instanceOperations =
          _record.getListField(InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name());
      List<InstanceOperation> newDeserializedInstanceOperations = new ArrayList<>();
      if (instanceOperations != null) {
        for (String serializedInstanceOperation : instanceOperations) {
          try {
            Map<String, String> properties = _objectMapper.readValue(serializedInstanceOperation,
                    new TypeReference<Map<String, String>>() {
                    });
            newDeserializedInstanceOperations.add(new InstanceOperation(properties));
          } catch (JsonProcessingException e) {
            _logger.error(
                "Failed to deserialize instance operation for instance: " + _record.getId(), e);
          }
        }
      }
      _deserializedInstanceOperations = newDeserializedInstanceOperations;
    }

    return _deserializedInstanceOperations;
  }

  /**
   * Set the instance operation for this instance.
   * This method also sets the HELIX_ENABLED, HELIX_DISABLED_REASON, and HELIX_DISABLED_TYPE fields
   * for backwards compatibility.
   *
   * @param operation the instance operation
   */
  public void setInstanceOperation(InstanceOperation operation) {
    List<InstanceOperation> deserializedInstanceOperations = getInstanceOperations();

    if (operation.getSource() == InstanceConstants.InstanceOperationSource.ADMIN) {
      deserializedInstanceOperations.clear();
    } else {
      // Remove the instance operation with the same source if it exists.
      deserializedInstanceOperations.removeIf(
          instanceOperation -> instanceOperation.getSource() == operation.getSource());
    }
    if (operation.getOperation() == InstanceConstants.InstanceOperation.ENABLE) {
      // Insert the operation after the last ENABLE or at the beginning if there isn't ENABLE in the list.
      int insertIndex = 0;
      for (int i = deserializedInstanceOperations.size() - 1; i >= 0; i--) {
        if (deserializedInstanceOperations.get(i).getOperation()
            == InstanceConstants.InstanceOperation.ENABLE) {
          insertIndex = i + 1;
          break;
        }
      }
      deserializedInstanceOperations.add(insertIndex, operation);
    } else {
      deserializedInstanceOperations.add(operation);
    }
    // Set the actual field in the ZnRecord
    _record.setListField(InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name(),
        deserializedInstanceOperations.stream().map(instanceOperation -> {
          try {
            return _objectMapper.writeValueAsString(instanceOperation.getProperties());
          } catch (JsonProcessingException e) {
            throw new HelixException(
                "Failed to serialize instance operation for instance: " + _record.getId()
                    + " Can't set the instance operation to: " + operation.getOperation(), e);
          }
        }).collect(Collectors.toList()));

    // TODO: Remove this when we are sure that all users are using the new InstanceOperation only and HELIX_ENABLED is removed.
    if (operation.getOperation() == InstanceConstants.InstanceOperation.DISABLE) {
      // We are still setting the HELIX_ENABLED field for backwards compatibility.
      // It is possible that users will be using earlier version of HelixAdmin or helix-rest
      // is on older version.

      if (_record.getBooleanField(InstanceConfigProperty.HELIX_ENABLED.name(), true)) {
        // Check if it is already disabled, if yes, then we don't need to set HELIX_ENABLED and HELIX_ENABLED_TIMESTAMP
        setInstanceEnabledHelper(false, operation.getTimestamp());
      }

      _record.setSimpleField(InstanceConfigProperty.HELIX_DISABLED_REASON.name(),
          operation.getReason());
      _record.setSimpleField(InstanceConfigProperty.HELIX_DISABLED_TYPE.name(),
          InstanceConstants.InstanceDisabledType.DEFAULT_INSTANCE_DISABLE_TYPE.name());
    } else if (operation.getOperation() == InstanceConstants.InstanceOperation.ENABLE) {
      // If any of the other InstanceOperations are of type DISABLE, set that in the HELIX_ENABLED,
      // HELIX_DISABLED_REASON, and HELIX_DISABLED_TYPE fields.
      InstanceOperation latestDisableInstanceOperation = null;
      for (InstanceOperation instanceOperation : getInstanceOperations()) {
        if (instanceOperation.getOperation() == InstanceConstants.InstanceOperation.DISABLE && (
            latestDisableInstanceOperation == null || instanceOperation.getTimestamp()
                > latestDisableInstanceOperation.getTimestamp())) {
          latestDisableInstanceOperation = instanceOperation;
        }
      }

      if (latestDisableInstanceOperation != null) {
        _record.setSimpleField(InstanceConfigProperty.HELIX_DISABLED_REASON.name(),
            latestDisableInstanceOperation.getReason());
        _record.setSimpleField(InstanceConfigProperty.HELIX_DISABLED_TYPE.name(),
            InstanceConstants.InstanceDisabledType.DEFAULT_INSTANCE_DISABLE_TYPE.name());
      } else {
        setInstanceEnabledHelper(true, operation.getTimestamp());
      }
    }
  }

  /**
   * Set the instance operation for this instance. Provide the InstanceOperation enum and the reason
   * and source will be set to default values.
   *
   * @param operation the instance operation
   */
  public void setInstanceOperation(InstanceConstants.InstanceOperation operation) {
    InstanceOperation instanceOperation =
        new InstanceOperation.Builder().setOperation(operation).build();
    setInstanceOperation(instanceOperation);
  }

  private void setInstanceOperationInit(InstanceConstants.InstanceOperation operation) {
    if (operation == null) {
      return;
    }
    InstanceOperation instanceOperation =
        new InstanceOperation.Builder().setOperation(operation).setReason("INIT").build();
    // When an instance is created for the first time the timestamp is set to -1 so that if it
    // is disabled it will not be considered within the delay window when it joins.
    instanceOperation.setTimestamp(HELIX_ENABLED_TIMESTAMP_DEFAULT_VALUE);
    setInstanceOperation(instanceOperation);
  }

  private InstanceOperation getActiveInstanceOperation() {
    List<InstanceOperation> instanceOperations = getInstanceOperations();

    if (instanceOperations.isEmpty()) {
      InstanceOperation instanceOperation =
          new InstanceOperation.Builder().setOperation(InstanceConstants.InstanceOperation.ENABLE)
              .setSource(InstanceConstants.InstanceOperationSource.DEFAULT).build();
      instanceOperation.setTimestamp(HELIX_ENABLED_TIMESTAMP_DEFAULT_VALUE);
      return instanceOperation;
    }

    // The last instance operation in the list is the most recent one.
    // ENABLE operation should not be included in the list.
    return instanceOperations.get(instanceOperations.size() - 1);
  }

  /**
   * Get the InstanceOperationType of this instance, default is ENABLE if nothing is set. If
   * HELIX_ENABLED is set to false, then the instance operation is DISABLE for backwards
   * compatibility.
   *
   * @return the instance operation
   */
  public InstanceOperation getInstanceOperation() {
    InstanceOperation activeInstanceOperation = getActiveInstanceOperation();
    try {
      activeInstanceOperation.getOperation();
    } catch (IllegalArgumentException e) {
      _logger.error("Invalid instance operation type for instance: " + _record.getId()
          + ". You may need to update your version of Helix to get support for this "
          + "type of InstanceOperation. Defaulting to UNKNOWN.");
      activeInstanceOperation =
          new InstanceOperation.Builder().setOperation(InstanceConstants.InstanceOperation.UNKNOWN)
              .build();
    }

    // Always respect the HELIX_ENABLED being set to false when instance operation is unset
    // for backwards compatibility.
    if (!_record.getBooleanField(InstanceConfigProperty.HELIX_ENABLED.name(),
        HELIX_ENABLED_DEFAULT_VALUE)
        && (InstanceConstants.INSTANCE_DISABLED_OVERRIDABLE_OPERATIONS.contains(
        activeInstanceOperation.getOperation()))) {
      return new InstanceOperation.Builder().setOperation(
              InstanceConstants.InstanceOperation.DISABLE).setReason(getInstanceDisabledReason())
          .setSource(
              InstanceConstants.InstanceOperationSource.instanceDisabledTypeToInstanceOperationSource(
                  InstanceConstants.InstanceDisabledType.valueOf(getInstanceDisabledType())))
          .build();
    }

    return activeInstanceOperation;
  }

  /**
   * Check if this instance is enabled. This is used to determine if the instance can host online
   * replicas and take new assignment.
   *
   * @return true if enabled, false otherwise
   */
  public boolean getInstanceEnabled() {
    return getInstanceOperation().getOperation().equals(InstanceConstants.InstanceOperation.ENABLE);
  }

  /**
   * Check to see if the instance is assignable. This is used to determine if the instance can be
   * selected by the rebalancer to take assignment of replicas.
   *
   * @return true if the instance is assignable, false otherwise
   */
  public boolean isAssignable() {
    return InstanceConstants.ASSIGNABLE_INSTANCE_OPERATIONS.contains(
        getInstanceOperation().getOperation());
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
  @Deprecated
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
    Map<String, String> newDisabledMap =
        _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    if (newDisabledMap == null && oldDisabled == null) {
      return null;
    }

    Set<String> disabledPartitions = new HashSet<String>();
    if (oldDisabled != null) {
      disabledPartitions.addAll(oldDisabled);
    }

    if (newDisabledMap != null) {
      for (String perResource : newDisabledMap.values()) {
        disabledPartitions.addAll(HelixUtil.deserializeByComma(perResource));
      }
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
    Map<String, String> newDisabledMap =
        _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    if ((newDisabledMap == null || !newDisabledMap.containsKey(resourceName))
        && oldDisabled == null) {
      return null;
    }

    Set<String> disabledPartitions = new HashSet<String>();
    if (oldDisabled != null) {
      disabledPartitions.addAll(oldDisabled);
    }

    if (newDisabledMap != null && newDisabledMap.containsKey(resourceName)) {
      disabledPartitions.addAll(HelixUtil.deserializeByComma(newDisabledMap.get(resourceName)));
    }
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

    // if all resource key, then thats the map

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
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
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
    _record.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name(), list);
  }

  public void setInstanceEnabledForPartition(String resourceName, String partitionName,
      boolean enabled) {
    // Get old disabled partitions if exists
    // TODO: Remove this when getDisabledParition() removed.
    List<String> oldDisabledPartitions =
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());

    Map<String, String> currentDisabled =
        _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name());
    Set<String> disabledPartitions = new HashSet<>();

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

    List<String> disabledPartitionList = new ArrayList<>(disabledPartitions);
    Collections.sort(disabledPartitionList);
    if (currentDisabled == null) {
      currentDisabled = new HashMap<>();
    }

    if (disabledPartitionList != null && !disabledPartitionList.isEmpty()) {
      currentDisabled.put(resourceName, HelixUtil.serializeByComma(disabledPartitionList));
    } else {
      currentDisabled.remove(resourceName);
    }

    if (!currentDisabled.isEmpty()) {
      _record.setMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name(), currentDisabled);
    }

    if (oldDisabledPartitions != null && !oldDisabledPartitions.isEmpty()) {
      _record.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.name(),
          oldDisabledPartitions);
    }
  }

  public boolean isInstanceInDomain(String domain) {
    if (domain == null) {
      throw new HelixException("Invalid input for domain.");
    }

    if (_record.getSimpleField(InstanceConfigProperty.DOMAIN.name()) == null) {
      return false;
    }

    Set<String> domainSet = new HashSet<>(Arrays.asList(domain.split(",")));
    Set<String> instanceDomains = new HashSet<>(
        Arrays.asList(_record.getSimpleField(InstanceConfigProperty.DOMAIN.name()).split(",")));
    domainSet.removeAll(instanceDomains);
    return domainSet.size() == 0;
  }

  /**
   * Whether the delay rebalance is enabled for this instance.
   * By default, it is enable if the field is not set.
   *
   * @return
   */
  public boolean isDelayRebalanceEnabled() {
    return _record
        .getBooleanField(ResourceConfig.ResourceConfigProperty.DELAY_REBALANCE_ENABLED.name(),
            true);
  }

  /**
   * Enable/Disable the delayed rebalance. By default it is enabled if not set.
   *
   * @param enabled
   */
  public void setDelayRebalanceEnabled(boolean enabled) {
    _record.setBooleanField(ResourceConfig.ResourceConfigProperty.DELAY_REBALANCE_ENABLED.name(),
        enabled);
  }

  /**
   * Get maximum allowed running task count on this instance
   * @return the maximum task count
   */
  public int getMaxConcurrentTask() {
    return _record.getIntField(InstanceConfigProperty.MAX_CONCURRENT_TASK.name(), MAX_CONCURRENT_TASK_NOT_SET);
  }

  public void setMaxConcurrentTask(int maxConcurrentTask) {
    _record.setIntField(InstanceConfigProperty.MAX_CONCURRENT_TASK.name(), maxConcurrentTask);
  }

  /**
   * Get the target size of task thread pool.
   * @return the target size of task thread pool
   */
  public int getTargetTaskThreadPoolSize() {
    return _record
        .getIntField(InstanceConfig.InstanceConfigProperty.TARGET_TASK_THREAD_POOL_SIZE.name(),
            TARGET_TASK_THREAD_POOL_SIZE_NOT_SET);
  }

  /**
   * Set the target size of task thread pool.
   * @param targetTaskThreadPoolSize - the new target task thread pool size
   * @throws IllegalArgumentException - when the provided new thread pool size is negative
   */
  public void setTargetTaskThreadPoolSize(int targetTaskThreadPoolSize)
      throws IllegalArgumentException {
    if (targetTaskThreadPoolSize < 0) {
      throw new IllegalArgumentException("targetTaskThreadPoolSize must be non-negative!");
    }
    _record.setIntField(InstanceConfig.InstanceConfigProperty.TARGET_TASK_THREAD_POOL_SIZE.name(),
        targetTaskThreadPoolSize);
  }

  /**
   * Get the instance information map from the map fields.
   * @return data map if it exists, or empty map
   */
  public Map<String, String> getInstanceInfoMap() {
    Map<String, String> instanceInfoMap =
        _record.getMapField(InstanceConfigProperty.INSTANCE_INFO_MAP.name());
    return instanceInfoMap != null ? instanceInfoMap : Collections.emptyMap();
  }

  /**
   * Set instanceInfoMap to map of information about the instance that can be used
   * to construct the DOMAIN field.
   * @param instanceInfoMap Map of information about the instance. ie: { 'rack': 'rack-1', 'host': 'host-1' }
   */
  private void setInstanceInfoMap(Map<String, String> instanceInfoMap) {
    if (instanceInfoMap == null) {
      _record.getMapFields().remove(InstanceConfigProperty.INSTANCE_INFO_MAP.name());
    } else {
      _record.setMapField(InstanceConfigProperty.INSTANCE_INFO_MAP.name(), instanceInfoMap);
    }
  }

  /**
   * Get the instance capacity information from the map fields.
   * @return data map if it exists, or empty map
   */
  public Map<String, Integer> getInstanceCapacityMap() {
    Map<String, String> capacityData =
        _record.getMapField(InstanceConfigProperty.INSTANCE_CAPACITY_MAP.name());

    if (capacityData != null) {
      return capacityData.entrySet().stream().collect(
          Collectors.toMap(entry -> entry.getKey(), entry -> Integer.parseInt(entry.getValue())));
    }
    return Collections.emptyMap();
  }

  /**
   * Set the instance capacity information with an Integer mapping.
   * @param capacityDataMap - map of instance capacity data
   *                        If null, the capacity map item will be removed from the config.
   * @throws IllegalArgumentException - when any of the data value is a negative number
   *
   * This information is required by the global rebalancer.
   * @see <a href="Rebalance Algorithm">
   *   https://github.com/apache/helix/wiki/Weight-Aware-Globally-Even-Distribute-Rebalancer#rebalance-algorithm-adapter
   *   </a>
   * If the instance capacity is not configured in neither Instance Config nor Cluster Config, the
   * cluster topology is considered invalid. So the rebalancer may stop working.
   * Note that when a rebalancer requires this capacity information, it will ignore INSTANCE_WEIGHT.
   */
  public void setInstanceCapacityMap(Map<String, Integer> capacityDataMap)
      throws IllegalArgumentException {
    if (capacityDataMap == null) {
      _record.getMapFields().remove(InstanceConfigProperty.INSTANCE_CAPACITY_MAP.name());
    } else {
      Map<String, String> capacityData = new HashMap<>();
      capacityDataMap.forEach((key, value) -> {
        if (value < 0) {
          throw new IllegalArgumentException(
              String.format("Capacity Data contains a negative value: %s = %d", key, value));
        }
        capacityData.put(key, Integer.toString(value));
      });
      _record.setMapField(InstanceConfigProperty.INSTANCE_CAPACITY_MAP.name(), capacityData);
    }
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

  /**
   * Get the logicalId of this instance. If it does not exist or is not set,
   * return the instance name.
   * @param logicalIdKey the key for the DOMAIN field containing the logicalId
   * @return the logicalId of this instance
   */
  public String getLogicalId(String logicalIdKey) {
    // TODO: Consider caching DomainMap, parsing the DOMAIN string every time
    // getLogicalId is called can become expensive if called too frequently.
    return getDomainAsMap().getOrDefault(logicalIdKey, getInstanceName());
  }

  @Override
  public boolean isValid() {
    // HELIX-65: remove check for hostname/port existence
    return true;
  }

  /**
   * Create InstanceConfig with given instanceId, instanceId should be in format of host:port
   * @param instanceId
   * @return
   */
  public static InstanceConfig toInstanceConfig(String instanceId) {
    String host = null;
    int port = -1;
    // to maintain backward compatibility we parse string of format host:port
    // and host_port, where host port must be of type string and int
    char[] delims = new char[] {
        ':', '_'
    };
    for (char delim : delims) {
      String regex = String.format("(.*)[%c]([\\d]+)", delim);
      if (instanceId.matches(regex)) {
        int lastIndexOf = instanceId.lastIndexOf(delim);
        try {
          port = Integer.parseInt(instanceId.substring(lastIndexOf + 1));
          host = instanceId.substring(0, lastIndexOf);
        } catch (Exception e) {
          _logger.warn("Unable to extract host and port from instanceId:" + instanceId);
        }
        break;
      }
    }
    if (host != null && port > 0) {
      instanceId = host + "_" + port;
    }
    InstanceConfig config = new InstanceConfig(instanceId);
    if (host != null && port > 0) {
      config.setHostName(host);
      config.setPort(String.valueOf(port));

    }

    if (config.getHostName() == null) {
      config.setHostName(instanceId);
    }
    return config;
  }

  /**
   * Validate if the topology related settings (Domain or ZoneId) in the given instanceConfig
   * are valid and align with current clusterConfig.
   * This function should be called when instance added to cluster or caller updates instanceConfig.
   *
   * @throws IllegalArgumentException
   */
  public boolean validateTopologySettingInInstanceConfig(ClusterConfig clusterConfig,
      String instanceName) {
    //IllegalArgumentException will be thrown here if the input is not valid.
    Topology.computeInstanceTopologyMap(clusterConfig, instanceName, this,
        false /*earlyQuitForFaultZone*/);
    return true;
  }

  /**
   * Overwrite the InstanceConfigProperties from the given InstanceConfig to this InstanceConfig.
   * The merge is done by overwriting the properties in this InstanceConfig with the properties
   * from the given InstanceConfig. {@link #NON_OVERWRITABLE_PROPERTIES} will not be overridden.
   *
   * @param overwritingInstanceConfig the InstanceConfig to override into this InstanceConfig
   */
  public void overwriteInstanceConfig(InstanceConfig overwritingInstanceConfig) {
    // Remove all overwritable fields from the record
    Set<String> overwritableProperties = Arrays.stream(InstanceConfigProperty.values())
        .filter(property -> !NON_OVERWRITABLE_PROPERTIES.contains(property)).map(Enum::name)
        .collect(Collectors.toSet());
    _record.getSimpleFields().keySet().removeAll(overwritableProperties);
    _record.getListFields().keySet().removeAll(overwritableProperties);
    _record.getMapFields().keySet().removeAll(overwritableProperties);

    // Get all overwritable fields from the overwritingInstanceConfig and set them in this record
    overwritingInstanceConfig.getRecord().getSimpleFields().entrySet().stream()
        .filter(entry -> overwritableProperties.contains(entry.getKey()))
        .forEach((entry) -> _record.setSimpleField(entry.getKey(), entry.getValue()));
    overwritingInstanceConfig.getRecord().getListFields().entrySet().stream()
        .filter(entry -> overwritableProperties.contains(entry.getKey()))
        .forEach((entry) -> _record.setListField(entry.getKey(), entry.getValue()));
    overwritingInstanceConfig.getRecord().getMapFields().entrySet().stream()
        .filter(entry -> overwritableProperties.contains(entry.getKey()))
        .forEach((entry) -> _record.setMapField(entry.getKey(), entry.getValue()));
  }

  public static class Builder {
    private String _hostName;
    private String _port;
    private String _domain;
    private int _weight = WEIGHT_NOT_SET;
    private List<String> _tags = new ArrayList<>();
    private boolean _instanceEnabled = HELIX_ENABLED_DEFAULT_VALUE;
    private InstanceConstants.InstanceOperation _instanceOperation;
    private Map<String, String> _instanceInfoMap;
    private Map<String, Integer> _instanceCapacityMap;

    /**
     * Build a new InstanceConfig with given instanceId
     * @param instanceId A unique ID for this instance
     * @return InstanceConfig
     */
    public InstanceConfig build(String instanceId) {
      InstanceConfig instanceConfig = new InstanceConfig(instanceId);

      String proposedHostName = instanceId;
      String proposedPort = "";
      int lastPos = instanceId.lastIndexOf("_");
      if (lastPos > 0) {
        proposedHostName = instanceId.substring(0, lastPos);
        proposedPort = instanceId.substring(lastPos + 1);
      }

      if (_hostName != null) {
        instanceConfig.setHostName(_hostName);
      } else {
        instanceConfig.setHostName(proposedHostName);
      }

      if (_port != null) {
        instanceConfig.setPort(_port);
      } else {
        instanceConfig.setPort(proposedPort);
      }

      if (_domain != null) {
        instanceConfig.setDomain(_domain);
      }

      if (_weight != InstanceConfig.WEIGHT_NOT_SET) {
        instanceConfig.setWeight(_weight);
      }

      for (String tag : _tags) {
        instanceConfig.addTag(tag);
      }

      if (_instanceOperation == null && !_instanceEnabled) {
        instanceConfig.setInstanceOperationInit(InstanceConstants.InstanceOperation.DISABLE);
      }

      if (_instanceOperation != null && !_instanceOperation.equals(
          InstanceConstants.InstanceOperation.ENABLE)) {
        instanceConfig.setInstanceOperationInit(_instanceOperation);
      }

      if (_instanceInfoMap != null) {
        instanceConfig.setInstanceInfoMap(_instanceInfoMap);
      }

      if (_instanceCapacityMap != null) {
        instanceConfig.setInstanceCapacityMap(_instanceCapacityMap);
      }

      return instanceConfig;
    }

    /**
     * Set the host name for this instance
     * @param hostName the host name
     * @return InstanceConfig.Builder
     */
    public Builder setHostName(String hostName) {
      _hostName = hostName;
      return this;
    }

    /**
     * Set the port for this instance
     * @param port the Helix port
     * @return InstanceConfig.Builder
     */
    public Builder setPort(String port) {
      _port = port;
      return this;
    }

    /**
     * Set the domain for this instance
     * @param domain the domain
     * @return InstanceConfig.Builder
     */
    public Builder setDomain(String domain) {
      _domain = domain;
      return this;
    }

    /**
     * Set the weight for this instance
     * @param weight the weight
     * @return InstanceConfig.Builder
     */
    public Builder setWeight(int weight) {
      _weight = weight;
      return this;
    }

    /**
     * Add a tag for this instance
     * @param tag the tag
     * @return InstanceConfig.Builder
     */
    public Builder addTag(String tag) {
      _tags.add(tag);
      return this;
    }

    /**
     * Set the enabled status for this instance
     * @deprecated HELIX_ENABLED is no longer in use. Use setInstanceOperation instead.
     * @param instanceEnabled true if enabled, false otherwise
     * @return InstanceConfig.Builder
     */
    @Deprecated
    public Builder setInstanceEnabled(boolean instanceEnabled) {
      _instanceEnabled = instanceEnabled;
      return this;
    }

    /**
     * Set the instance operation for this instance
     *
     * @param instanceOperation the instance operation.
     * @return InstanceConfig.Builder
     */
    public Builder setInstanceOperation(InstanceConstants.InstanceOperation instanceOperation) {
      _instanceOperation = instanceOperation;
      return this;
    }

    /**
     * Set the INSTANCE_INFO_MAP for this instance
     * @param instanceInfoMap the instance info map
     * @return InstanceConfig.Builder
     */
    public Builder setInstanceInfoMap(Map<String, String> instanceInfoMap) {
      _instanceInfoMap = instanceInfoMap;
      return this;
    }

    /**
     * Add instance info to the INSTANCE_INFO_MAP.
     * Only adds if the key does not already exist.
     * @param key the key for the information
     * @param value the value the information
     * @return InstanceConfig.Builder
     */
    public Builder addInstanceInfo(String key, String value) {
      if (_instanceInfoMap == null) {
        _instanceInfoMap = new HashMap<>();
      }
      _instanceInfoMap.putIfAbsent(key, value);
      return this;
    }

    /**
     * Set the capacity map for this instance
     * @param instanceCapacityMap the capacity map
     * @return InstanceConfig.Builder
     */
    public Builder setInstanceCapacityMap(Map<String, Integer> instanceCapacityMap) {
      _instanceCapacityMap = instanceCapacityMap;
      return this;
    }
  }
}
