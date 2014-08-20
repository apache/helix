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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.NamespacedConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.log4j.Logger;

import com.google.common.base.Enums;
import com.google.common.base.Optional;

/**
 * Instance configurations
 */
public class InstanceConfig extends HelixProperty {
  private static final Logger LOG = Logger.getLogger(InstanceConfig.class);

  /**
   * Configurable characteristics of an instance
   */
  public enum InstanceConfigProperty {
    HELIX_HOST,
    HELIX_PORT,
    HELIX_ENABLED,
    HELIX_DISABLED_PARTITION,
    TAG_LIST,
    CONTAINER_SPEC,
    CONTAINER_STATE,
    CONTAINER_ID
  }

  /**
   * Instantiate for a specific instance
   * @param instanceId the instance identifier
   */
  public InstanceConfig(String instanceId) {
    super(instanceId);
  }

  /**
   * Instantiate for a specific instance
   * @param participantId the instance identifier
   */
  public InstanceConfig(ParticipantId participantId) {
    super(participantId.stringify());
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
   * @param partition the partition name to check
   * @return true if the instance is enabled for the partition, false otherwise
   */
  public boolean getInstanceEnabledForPartition(String partition) {
    // Map<String, String> disabledPartitionMap =
    // _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
    List<String> disabledPartitions =
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
    if (disabledPartitions != null && disabledPartitions.contains(partition)) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Get the partitions disabled by this instance
   * @return a list of partition names
   */
  public List<String> getDisabledPartitions() {
    List<String> disabledPartitions =
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
    if (disabledPartitions == null) {
      disabledPartitions = Collections.emptyList();
    }
    return disabledPartitions;
  }

  /**
   * Set the enabled state for a partition on this instance
   * @param partitionName the partition to set
   * @param enabled true to enable, false to disable
   */
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

  /**
   * Set the enabled state for a partition on this instance
   * @param partitionId the partition to set
   * @param enabled true to enable, false to disable
   */
  public void setParticipantEnabledForPartition(PartitionId partitionId, boolean enabled) {
    setInstanceEnabledForPartition(partitionId.stringify(), enabled);
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
   * Get the identifier of this participant
   * @return the participant id
   */
  public ParticipantId getParticipantId() {
    return ParticipantId.from(getInstanceName());
  }

  /**
   * Get a backward-compatible participant user config
   * @return UserConfig
   */
  public UserConfig getUserConfig() {
    UserConfig userConfig = UserConfig.from(this);
    try {
      for (String simpleField : _record.getSimpleFields().keySet()) {
        Optional<InstanceConfigProperty> enumField =
            Enums.getIfPresent(InstanceConfigProperty.class, simpleField);
        Optional<HelixPropertyAttribute> superEnumField =
            Enums.getIfPresent(HelixPropertyAttribute.class, simpleField);
        if (!simpleField.contains(NamespacedConfig.PREFIX_CHAR + "") && !enumField.isPresent()
            && !superEnumField.isPresent()) {
          userConfig.setSimpleField(simpleField, _record.getSimpleField(simpleField));
        }
      }
      for (String listField : _record.getListFields().keySet()) {
        Optional<InstanceConfigProperty> enumField =
            Enums.getIfPresent(InstanceConfigProperty.class, listField);
        if (!listField.contains(NamespacedConfig.PREFIX_CHAR + "") && !enumField.isPresent()) {
          userConfig.setListField(listField, _record.getListField(listField));
        }
      }
      for (String mapField : _record.getMapFields().keySet()) {
        Optional<InstanceConfigProperty> enumField =
            Enums.getIfPresent(InstanceConfigProperty.class, mapField);
        if (!mapField.contains(NamespacedConfig.PREFIX_CHAR + "") && !enumField.isPresent()) {
          userConfig.setMapField(mapField, _record.getMapField(mapField));
        }
      }
    } catch (NoSuchMethodError e) {
      LOG.error("Could not parse InstanceConfig for additional user config");
    }
    return userConfig;
  }

  public void setContainerSpec(ContainerSpec spec) {
    if (spec != null) {
      _record.setSimpleField(InstanceConfigProperty.CONTAINER_SPEC.toString(), spec.toString());
    }
  }

  public ContainerSpec getContainerSpec() {
    return ContainerSpec.from(_record.getSimpleField(InstanceConfigProperty.CONTAINER_SPEC
        .toString()));
  }

  public void setContainerState(ContainerState state) {
    _record.setEnumField(InstanceConfigProperty.CONTAINER_STATE.toString(), state);
  }

  public ContainerState getContainerState() {
    return _record.getEnumField(InstanceConfigProperty.CONTAINER_STATE.toString(),
        ContainerState.class, null);
  }

  public void setContainerId(ContainerId containerId) {
    if (containerId != null) {
      _record
          .setSimpleField(InstanceConfigProperty.CONTAINER_ID.toString(), containerId.toString());
    }
  }

  public ContainerId getContainerId() {
    return ContainerId.from(_record.getSimpleField(InstanceConfigProperty.CONTAINER_ID.toString()));
  }

  @Override
  public boolean isValid() {
    // HELIX-65: remove check for hostname/port existence
    return true;
  }
}
