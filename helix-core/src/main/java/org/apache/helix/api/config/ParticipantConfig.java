package org.apache.helix.api.config;

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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Configuration properties of a Helix participant
 */
public class ParticipantConfig {
  private final InstanceConfig _instanceConfig;
  private final Set<PartitionId> _disabledPartitions;
  private final Set<String> _tags;
  private final UserConfig _userConfig;

  /**
   * Initialize a participant configuration. Also see ParticipantConfig.Builder
   * @param id participant id
   * @param hostName host where participant can be reached
   * @param port port to use to contact participant
   * @param isEnabled true if enabled, false if disabled
   * @param disabledPartitions set of partitions, if any to disable on this participant
   * @param tags tags to set for the participant
   */
  private ParticipantConfig(InstanceConfig instanceConfig) {
    _instanceConfig = instanceConfig;
    _disabledPartitions = Sets.newHashSet();
    for (String partitionName : instanceConfig.getDisabledPartitions()) {
      _disabledPartitions.add(PartitionId.from(partitionName));
    }
    _tags = Sets.newHashSet(instanceConfig.getTags());
    _userConfig = instanceConfig.getUserConfig();
  }

  /**
   * Get the host name of the participant
   * @return host name, or null if not applicable
   */
  public String getHostName() {
    return _instanceConfig.getHostName();
  }

  /**
   * Get the port of the participant
   * @return port number, or -1 if not applicable
   */
  public int getPort() {
    return _instanceConfig.getRecord()
        .getIntField(InstanceConfigProperty.HELIX_PORT.toString(), -1);
  }

  /**
   * Get if the participant is enabled
   * @return true if enabled or false otherwise
   */
  public boolean isEnabled() {
    return _instanceConfig.getInstanceEnabled();
  }

  /**
   * Get disabled partition id's
   * @return set of disabled partition id's, or empty set if none
   */
  public Set<PartitionId> getDisabledPartitions() {
    return _disabledPartitions;
  }

  /**
   * Get tags
   * @return set of tags
   */
  public Set<String> getTags() {
    return _tags;
  }

  /**
   * Check if participant has a tag
   * @param tag tag to check
   * @return true if tagged, false otherwise
   */
  public boolean hasTag(String tag) {
    return _tags.contains(tag);
  }

  /**
   * Get user-specified configuration properties of this participant
   * @return UserConfig properties
   */
  public UserConfig getUserConfig() {
    return _userConfig;
  }

  /**
   * Get the participant id
   * @return ParticipantId
   */
  public ParticipantId getId() {
    return _instanceConfig.getParticipantId();
  }

  /**
   * Get the physical instance config
   * @return InstanceConfig
   */
  public InstanceConfig getInstanceConfig() {
    return _instanceConfig;
  }

  /**
   * Update context for a ParticipantConfig
   */
  public static class Delta {
    private final InstanceConfig _updatedConfig;
    private final Set<String> _removedTags;
    private final Set<String> _removedDisabledPartitions;

    /**
     * Instantiate the delta for a participant config
     * @param participantId the participant to update
     */
    public Delta(ParticipantId participantId) {
      _updatedConfig = new InstanceConfig(participantId);
      _removedTags = Sets.newHashSet();
      _removedDisabledPartitions = Sets.newHashSet();
    }

    /**
     * Set the participant host name
     * @param hostName reachable host when live
     * @return Delta
     */
    public Delta setHostName(String hostName) {
      _updatedConfig.setHostName(hostName);
      return this;
    }

    /**
     * Set the participant port
     * @param port port number
     * @return Delta
     */
    public Delta setPort(int port) {
      _updatedConfig.setPort(String.valueOf(port));
      return this;
    }

    /**
     * Set the enabled status of the participant
     * @param isEnabled true if enabled, false if disabled
     * @return Delta
     */
    public Delta setEnabled(boolean isEnabled) {
      _updatedConfig.setInstanceEnabled(isEnabled);
      return this;
    }

    /**
     * Set the user configuration
     * @param userConfig user-specified properties
     * @return Delta
     */
    public Delta addUserConfig(UserConfig userConfig) {
      _updatedConfig.addNamespacedConfig(userConfig);
      return this;
    }

    /**
     * Add an new tag for this participant
     * @param tag the tag to add
     * @return Delta
     */
    public Delta addTag(String tag) {
      _updatedConfig.addTag(tag);
      _removedTags.remove(tag);
      return this;
    }

    /**
     * Remove a tag for this participant
     * @param tag the tag to remove
     * @return Delta
     */
    public Delta removeTag(String tag) {
      _removedTags.add(tag);
      _updatedConfig.removeTag(tag);
      return this;
    }

    /**
     * Add a partition to disable for this participant
     * @param partitionId the partition to disable
     * @return Delta
     */
    public Delta addDisabledPartition(PartitionId partitionId) {
      _updatedConfig.setParticipantEnabledForPartition(partitionId, false);
      return this;
    }

    /**
     * Remove a partition from the disabled set for this participant
     * @param partitionId the partition to enable
     * @return Delta
     */
    public Delta removeDisabledPartition(PartitionId partitionId) {
      _removedDisabledPartitions.add(partitionId.stringify());
      return this;
    }

    /**
     * Merge the participant delta in using a physical accessor
     * @param accessor the physical accessor
     */
    public void merge(HelixDataAccessor accessor) {
      // Update the InstanceConfig but in place so that tags and disabled partitions can be
      // added/removed
      DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
        @Override
        public ZNRecord update(ZNRecord currentData) {
          currentData.getSimpleFields().putAll(_updatedConfig.getRecord().getSimpleFields());
          if (!_updatedConfig.getTags().isEmpty() || _removedTags.isEmpty()) {
            List<String> tags =
                currentData.getListField(InstanceConfigProperty.TAG_LIST.toString());
            if (tags != null) {
              tags.addAll(_updatedConfig.getTags());
              tags.removeAll(_removedTags);
              currentData.setListField(InstanceConfigProperty.TAG_LIST.toString(), tags);
            }
          }
          if (!_updatedConfig.getDisabledPartitions().isEmpty()
              || _removedDisabledPartitions.isEmpty()) {
            List<String> disabledPartitions =
                currentData
                    .getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
            if (disabledPartitions != null) {
              disabledPartitions.addAll(_updatedConfig.getDisabledPartitions());
              disabledPartitions.removeAll(_removedDisabledPartitions);
              currentData.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(),
                  disabledPartitions);
            }
          }
          return currentData;
        }
      };
      List<String> paths =
          Arrays.asList(accessor.keyBuilder().instanceConfig(_updatedConfig.getId()).getPath());
      List<DataUpdater<ZNRecord>> updaters = Lists.newArrayList();
      updaters.add(updater);
      accessor.updateChildren(paths, updaters, AccessOption.PERSISTENT);
    }
  }

  /**
   * Assemble a participant
   */
  public static class Builder {
    private final InstanceConfig _instanceConfig;

    /**
     * Build a participant with a given id
     * @param id participant id
     */
    public Builder(ParticipantId id) {
      _instanceConfig = new InstanceConfig(id);
    }

    /**
     * Set the participant host name
     * @param hostName reachable host when live
     * @return Builder
     */
    public Builder hostName(String hostName) {
      _instanceConfig.setHostName(hostName);
      return this;
    }

    /**
     * Set the participant port
     * @param port port number
     * @return Builder
     */
    public Builder port(int port) {
      _instanceConfig.setPort(String.valueOf(port));
      return this;
    }

    /**
     * Set whether or not the participant is enabled
     * @param isEnabled true if enabled, false otherwise
     * @return Builder
     */
    public Builder enabled(boolean isEnabled) {
      _instanceConfig.setInstanceEnabled(isEnabled);
      return this;
    }

    /**
     * Add a partition to disable for this participant
     * @param partitionId the partition to disable
     * @return Builder
     */
    public Builder addDisabledPartition(PartitionId partitionId) {
      _instanceConfig.setInstanceEnabledForPartition(partitionId.toString(), false);
      return this;
    }

    /**
     * Add an arbitrary tag for this participant
     * @param tag the tag to add
     * @return Builder
     */
    public Builder addTag(String tag) {
      _instanceConfig.addTag(tag);
      return this;
    }

    /**
     * Set the user configuration
     * @param userConfig user-specified properties
     * @return Builder
     */
    public Builder userConfig(UserConfig userConfig) {
      _instanceConfig.addNamespacedConfig(userConfig);
      return this;
    }

    /**
     * Assemble the participant
     * @return instantiated Participant
     */
    public ParticipantConfig build() {
      return new ParticipantConfig(_instanceConfig);
    }
  }

  /**
   * Get a participant config from a physical instance config
   * @param instanceConfig the populated config
   * @return ParticipantConfig instance
   */
  public static ParticipantConfig from(InstanceConfig instanceConfig) {
    return new ParticipantConfig(instanceConfig);
  }
}
