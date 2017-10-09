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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import java.util.TreeMap;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.HelixConfigProperty;
import org.apache.helix.api.config.RebalanceConfig;
import org.apache.helix.api.config.StateTransitionTimeoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource configurations
 */
public class ResourceConfig extends HelixProperty {
  /**
   * Configurable characteristics of a resource
   */
  public enum ResourceConfigProperty {
    MONITORING_DISABLED, // Resource-level config, do not create Mbean and report any status for the resource.
    NUM_PARTITIONS,
    STATE_MODEL_DEF_REF,
    STATE_MODEL_FACTORY_NAME,
    REPLICAS,
    MIN_ACTIVE_REPLICAS,
    MAX_PARTITIONS_PER_INSTANCE,
    INSTANCE_GROUP_TAG,
    HELIX_ENABLED,
    RESOURCE_GROUP_NAME,
    RESOURCE_TYPE,
    GROUP_ROUTING_ENABLED,
    EXTERNAL_VIEW_DISABLED,
    DELAY_REBALANCE_ENABLED
  }

  public enum ResourceConfigConstants {
    ANY_LIVEINSTANCE
  }

  private static final Logger _logger = LoggerFactory.getLogger(ResourceConfig.class.getName());
  /**
   * Instantiate for a specific instance
   *
   * @param resourceId the instance identifier
   */
  public ResourceConfig(String resourceId) {
    super(resourceId);
  }

  /**
   * Instantiate with a pre-populated record
   *
   * @param record a ZNRecord corresponding to an instance configuration
   */
  public ResourceConfig(ZNRecord record) {
    super(record);
  }

  /**
   * Instantiate with a pre-populated record with new record id
   * @param record a ZNRecord corresponding to an instance configuration
   * @param id     new ZNRecord ID
   */
  public ResourceConfig(ZNRecord record, String id) {
    super(record, id);
  }

  public ResourceConfig(String resourceId, Boolean monitorDisabled, int numPartitions,
      String stateModelDefRef, String stateModelFactoryName, String numReplica,
      int minActiveReplica, int maxPartitionsPerInstance, String instanceGroupTag,
      Boolean helixEnabled, String resourceGroupName, String resourceType,
      Boolean groupRoutingEnabled, Boolean externalViewDisabled,
      RebalanceConfig rebalanceConfig, StateTransitionTimeoutConfig stateTransitionTimeoutConfig,
      Map<String, List<String>> listFields, Map<String, Map<String, String>> mapFields,
      Boolean p2pMessageEnabled) {
    super(resourceId);

    if (monitorDisabled != null) {
      _record.setBooleanField(ResourceConfigProperty.MONITORING_DISABLED.name(), monitorDisabled);
    }

    if (p2pMessageEnabled != null) {
      _record.setBooleanField(HelixConfigProperty.P2P_MESSAGE_ENABLED.name(), p2pMessageEnabled);
    }

    if (numPartitions > 0) {
      _record.setIntField(ResourceConfigProperty.NUM_PARTITIONS.name(), numPartitions);
    }

    if (stateModelDefRef != null) {
      _record.setSimpleField(ResourceConfigProperty.STATE_MODEL_DEF_REF.name(), stateModelDefRef);
    }

    if (stateModelFactoryName != null) {
      _record.setSimpleField(ResourceConfigProperty.STATE_MODEL_FACTORY_NAME.name(), stateModelFactoryName);
    }

    if (numReplica != null) {
      _record.setSimpleField(ResourceConfigProperty.REPLICAS.name(), numReplica);
    }

    if (minActiveReplica >= 0) {
      _record.setIntField(ResourceConfigProperty.MIN_ACTIVE_REPLICAS.name(), minActiveReplica);
    }

    if (maxPartitionsPerInstance >= 0) {
      _record.setIntField(ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name(), maxPartitionsPerInstance);
    }

    if (instanceGroupTag != null) {
      _record.setSimpleField(ResourceConfigProperty.INSTANCE_GROUP_TAG.name(), instanceGroupTag);
    }

    if (helixEnabled != null) {
      _record.setBooleanField(ResourceConfigProperty.HELIX_ENABLED.name(), helixEnabled);
    }

    if (resourceGroupName != null) {
      _record.setSimpleField(ResourceConfigProperty.RESOURCE_GROUP_NAME.name(), resourceGroupName);
    }

    if (resourceType != null) {
      _record.setSimpleField(ResourceConfigProperty.RESOURCE_TYPE.name(), resourceType);
    }

    if (groupRoutingEnabled != null) {
      _record.setBooleanField(ResourceConfigProperty.GROUP_ROUTING_ENABLED.name(),
          groupRoutingEnabled);
    }

    if (externalViewDisabled != null) {
      _record.setBooleanField(ResourceConfigProperty.EXTERNAL_VIEW_DISABLED.name(),
          externalViewDisabled);
    }

    if (rebalanceConfig != null) {
      putSimpleConfigs(rebalanceConfig.getConfigsMap());
    }

    if (stateTransitionTimeoutConfig != null) {
      putMapConfig(StateTransitionTimeoutConfig.StateTransitionTimeoutProperty.TIMEOUT.name(),
          stateTransitionTimeoutConfig.getTimeoutMap());
    }

    if (listFields != null) {
      _record.setListFields(listFields);
    }

    if (mapFields != null) {
      _record.setMapFields(mapFields);
    }
  }


  /**
   * Get the value of DisableMonitoring set.
   *
   * @return the MonitoringDisabled is true or false
   */
  public Boolean isMonitoringDisabled() {
    return _record.getBooleanField(ResourceConfigProperty.MONITORING_DISABLED.toString(), false);
  }

  /**
   * Whether the P2P state transition message is enabled for this resource.
   * By default it is disabled if not set.
   *
   * @return
   */
  public boolean isP2PMessageEnabled() {
    return _record.getBooleanField(HelixConfigProperty.P2P_MESSAGE_ENABLED.name(), false);
  }

  /**
   * Get the associated resource
   * @return the name of the resource
   */
  public String getResourceName() {
    return _record.getId();
  }

  /**
   * Get the number of partitions of this resource
   * @return the number of partitions
   */
  public int getNumPartitions() {
    return _record.getIntField(ResourceConfigProperty.NUM_PARTITIONS.name(), 0);
  }

  /**
   * Get the state model associated with this resource
   * @return an identifier of the state model
   */
  public String getStateModelDefRef() {
    return _record.getSimpleField(ResourceConfigProperty.STATE_MODEL_DEF_REF.name());
  }

  /**
   * Get the state model factory associated with this resource
   * @return state model factory name
   */
  public String getStateModelFactoryName() {
    return _record.getSimpleField(ResourceConfigProperty.STATE_MODEL_FACTORY_NAME.name());
  }

  /**
   * Get the number of replicas for each partition of this resource
   * @return number of replicas (as a string)
   */
  public String getNumReplica() {
    // TODO: use IdealState.getNumbReplica()?
    return _record.getSimpleField(ResourceConfigProperty.REPLICAS.name());
  }

  /**
   * Get the number of minimal active partitions for this resource.
   *
   * @return
   */
  public int getMinActiveReplica() {
    return _record.getIntField(ResourceConfigProperty.MIN_ACTIVE_REPLICAS.name(), -1);
  }

  public int getMaxPartitionsPerInstance() {
    return _record.getIntField(ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.toString(),
        Integer.MAX_VALUE);
  }

  /**
   * Check for a tag that will restrict assignment to instances with a matching tag
   * @return the group tag, or null if none is present
   */
  public String getInstanceGroupTag() {
    return _record.getSimpleField(ResourceConfigProperty.INSTANCE_GROUP_TAG.toString());
  }

  /**
   * Get if the resource is enabled or not
   * By default, it's enabled
   * @return true if enabled; false otherwise
   */
  public Boolean isEnabled() {
    return _record.getBooleanField(ResourceConfigProperty.HELIX_ENABLED.name(), true);
  }

  /**
   * Get the resource type
   * @return the resource type, or null if none is being set
   */
  public String getResourceType() {
    return _record.getSimpleField(ResourceConfigProperty.RESOURCE_TYPE.name());
  }

  /**
   * Get the resource group name
   *
   * @return
   */
  public String getResourceGroupName() {
    return _record.getSimpleField(ResourceConfigProperty.RESOURCE_GROUP_NAME.name());
  }

  /**
   * Get if the resource group routing feature is enabled or not
   * By default, it's disabled
   *
   * @return true if enabled; false otherwise
   */
  public Boolean isGroupRoutingEnabled() {
    return _record.getBooleanField(ResourceConfigProperty.GROUP_ROUTING_ENABLED.name(), false);
  }

  /**
   * If the external view for this resource is disabled. by default, it is false.
   *
   * @return true if the external view should be disabled for this resource.
   */
  public Boolean isExternalViewDisabled() {
    return _record.getBooleanField(ResourceConfigProperty.EXTERNAL_VIEW_DISABLED.name(), false);
  }

  /**
   * Get rebalance config for this resource.
   * @return
   */
  public RebalanceConfig getRebalanceConfig() {
    RebalanceConfig rebalanceConfig = new RebalanceConfig(_record);
    return rebalanceConfig;
  }

  public StateTransitionTimeoutConfig getStateTransitionTimeoutConfig() {
    return StateTransitionTimeoutConfig.fromRecord(_record);
  }


  /**
   * Get the user-specified preference lists for all partitions
   *
   * @return map of lists of instances for all partitions in this resource.
   */
  public Map<String, List<String>> getPreferenceLists() {
    return _record.getListFields();
  }

  /**
   * Get the user-specified preference list of a partition
   * @param partitionName the name of the partition
   * @return a list of instances that can serve replicas of the partition
   */
  public List<String> getPreferenceList(String partitionName) {
    List<String> instanceStateList = _record.getListField(partitionName);

    if (instanceStateList != null) {
      return instanceStateList;
    }

    return null;
  }

  /**
   * Set the user-specified preference lists for all partitions in this resource.
   *
   * @param instanceLists the map of instance preference lists.N
   */
  public void setPreferenceLists(Map<String, List<String>> instanceLists) {
    _record.setListFields(instanceLists);
  }

  /**
   * Put a set of simple configs.
   *
   * @param configsMap
   */
  public void putSimpleConfigs(Map<String, String> configsMap) {
    getRecord().getSimpleFields().putAll(configsMap);
  }

  /**
   * Get all simple configurations.
   *
   * @return all simple configurations.
   */
  public Map<String, String> getSimpleConfigs() {
    return Collections.unmodifiableMap(getRecord().getSimpleFields());
  }

  /**
   * Put a single simple config value.
   *
   * @param configKey
   * @param configVal
   */
  public void putSimpleConfig(String configKey, String configVal) {
    getRecord().getSimpleFields().put(configKey, configVal);
  }

  /**
   * Get a single simple config value.
   *
   * @param configKey
   * @return configuration value, or NULL if not exist.
   */
  public String getSimpleConfig(String configKey) {
    return getRecord().getSimpleFields().get(configKey);
  }

  /**
   * Put a single map config.
   *
   * @param configKey
   * @param configValMap
   */
  public void putMapConfig(String configKey, Map<String, String> configValMap) {
    getRecord().setMapField(configKey, configValMap);
  }

  /**
   * Get a single map config.
   *
   * @param configKey
   * @return configuration value map, or NULL if not exist.
   */
  public Map<String, String> getMapConfig(String configKey) {
    return getRecord().getMapField(configKey);
  }

  /**
   * Determine whether the given config key is in the simple config
   * @param configKey The key to check whether exists
   * @return True if exists, otherwise false
   */
  public boolean simpleConfigContains(String configKey) {
    return getRecord().getSimpleFields().containsKey(configKey);
  }

  /**
   * Determine whether the given config key is the map config
   * @param configKey The key to check whether exists
   * @return True if exists, otherwise false
   */
  public boolean mapConfigContains(String configKey) {
    return getRecord().getMapFields().containsKey(configKey);
  }

  /**
   * Get the stored map fields
   * @return a map of map fields
   */
  public Map<String, Map<String, String>> getMapConfigs() {
    return getRecord().getMapFields();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ResourceConfig) {
      ResourceConfig that = (ResourceConfig) obj;

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

  @Override
  public boolean isValid() {
    return true;
  }


  public static class Builder {
    private String _resourceId;
    private Boolean _monitorDisabled;
    private int _numPartitions;
    private String _stateModelDefRef;
    private String _stateModelFactoryName;
    private String _numReplica;
    private int _minActiveReplica = -1;
    private int _maxPartitionsPerInstance = -1;
    private String _instanceGroupTag;
    private Boolean _helixEnabled;
    private String _resourceGroupName;
    private String _resourceType;
    private Boolean _groupRoutingEnabled;
    private Boolean _externalViewDisabled;
    private Boolean _p2pMessageEnabled;
    private RebalanceConfig _rebalanceConfig;
    private StateTransitionTimeoutConfig _stateTransitionTimeoutConfig;
    private Map<String, List<String>> _preferenceLists;
    private Map<String, Map<String, String>> _mapFields;

    public Builder(String resourceId) {
      _resourceId = resourceId;
    }

    public Builder setMonitorDisabled(boolean monitorDisabled) {
      _monitorDisabled = monitorDisabled;
      return this;
    }

    /**
     * Enable/Disable the p2p state transition message for this resource.
     * By default it is disabled if not set.
     *
     * @param enabled
     */
    public Builder setP2PMessageEnabled(boolean enabled) {
      _p2pMessageEnabled = enabled;
      return this;
    }

    public Boolean isMonitorDisabled() {
      return _monitorDisabled;
    }

    public String getResourceId() {
      return _resourceId;
    }

    public int getNumPartitions() {
      return _numPartitions;
    }

    public Builder setNumPartitions(int numPartitions) {
      _numPartitions = numPartitions;
      return this;
    }

    public String getStateModelDefRef() {
      return _stateModelDefRef;
    }

    public Builder setStateModelDefRef(String stateModelDefRef) {
      _stateModelDefRef = stateModelDefRef;
      return this;
    }

    public String getStateModelFactoryName() {
      return _stateModelFactoryName;
    }

    public Builder setStateModelFactoryName(String stateModelFactoryName) {
      _stateModelFactoryName = stateModelFactoryName;
      return this;
    }

    public String getNumReplica() {
      return _numReplica;
    }

    public Builder setNumReplica(String numReplica) {
      _numReplica = numReplica;
      return this;
    }

    public Builder setNumReplica(int numReplica) {
      return setNumReplica(String.valueOf(numReplica));
    }

    public int getMinActiveReplica() {
      return _minActiveReplica;
    }

    public Builder setMinActiveReplica(int minActiveReplica) {
      _minActiveReplica = minActiveReplica;
      return this;
    }

    public int getMaxPartitionsPerInstance() {
      return _maxPartitionsPerInstance;
    }

    public Builder setMaxPartitionsPerInstance(int maxPartitionsPerInstance) {
      _maxPartitionsPerInstance = maxPartitionsPerInstance;
      return this;
    }

    public String getInstanceGroupTag() {
      return _instanceGroupTag;
    }

    public Builder setInstanceGroupTag(String instanceGroupTag) {
      _instanceGroupTag = instanceGroupTag;
      return this;
    }

    public Boolean isHelixEnabled() {
      return _helixEnabled;
    }

    public Builder setHelixEnabled(boolean helixEnabled) {
      _helixEnabled = helixEnabled;
      return this;
    }

    public String getResourceType() {
      return _resourceType;
    }

    public Builder setResourceType(String resourceType) {
      _resourceType = resourceType;
      return this;
    }

    public String getResourceGroupName() {
      return _resourceGroupName;
    }

    public Builder setResourceGroupName(String resourceGroupName) {
      _resourceGroupName = resourceGroupName;
      return this;
    }

    public Boolean isGroupRoutingEnabled() {
      return _groupRoutingEnabled;
    }

    public Builder setGroupRoutingEnabled(boolean groupRoutingEnabled) {
      _groupRoutingEnabled = groupRoutingEnabled;
      return this;
    }

    public Boolean isExternalViewDisabled() {
      return _externalViewDisabled;
    }

    public Builder setExternalViewDisabled(boolean externalViewDisabled) {
      _externalViewDisabled = externalViewDisabled;
      return this;
    }

    public Builder setRebalanceConfig(RebalanceConfig rebalanceConfig) {
      _rebalanceConfig = rebalanceConfig;
      return this;
    }

    public RebalanceConfig getRebalanceConfig() {
      return _rebalanceConfig;
    }

    public Builder setStateTransitionTimeoutConfig(
        StateTransitionTimeoutConfig stateTransitionTimeoutConfig) {
      _stateTransitionTimeoutConfig = stateTransitionTimeoutConfig;
      return this;
    }

    public StateTransitionTimeoutConfig getStateTransitionTimeoutConfig() {
      return _stateTransitionTimeoutConfig;
    }

    /**
     * Set the user-specified preference list of a partition
     *
     * @param partitionName the name of the partition
     * @param instanceList the instance preference list
     */
    public Builder setPreferenceList(String partitionName, List<String> instanceList) {
      if (_preferenceLists == null) {
        _preferenceLists = new TreeMap<>();
      }
      _preferenceLists.put(partitionName, instanceList);
      return this;
    }

    /**
     * Set the user-specified preference lists for all partitions in this resource.
     *
     * @param instanceLists the map of instance preference lists.N
     */
    public Builder setPreferenceLists(Map<String, List<String>> instanceLists) {
      _preferenceLists = new TreeMap<>(instanceLists);
      return this;
    }

    public Map<String, List<String>> getPreferenceLists() {
      return _preferenceLists;
    }

    public Builder setMapField(String key, Map<String, String> fields) {
      if (_mapFields == null) {
        _mapFields = new TreeMap<>();
      }
      _mapFields.put(key, fields);
      return this;
    }

    public Builder setMapFields(Map<String, Map<String, String>> mapFields) {
      _mapFields = mapFields;
      return this;
    }

    public Map<String, Map<String, String>> getMapFields() {
      return _mapFields;
    }

    private void validate() {
      if (_rebalanceConfig == null) {
        throw new IllegalArgumentException("RebalanceConfig not set!");
      } else {
        if (!_rebalanceConfig.isValid()) {
          throw new IllegalArgumentException("Invalid RebalanceConfig!");
        }
      }
      if (_numPartitions <= 0) {
        throw new IllegalArgumentException("Invalid number of partitions!");
      }

      if (_stateModelDefRef == null) {
        throw new IllegalArgumentException("State Model Definition Reference is not set!");
      }

      if (_numReplica == null) {
        throw new IllegalArgumentException("Number of replica is not set!");
      } else {
        if (!_numReplica.equals(ResourceConfigConstants.ANY_LIVEINSTANCE.name())) {
          try {
            Integer.parseInt(_numReplica);
          } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid number of replica!");
          }
        }
      }
    }

    public ResourceConfig build() {
      // TODO: Reenable the validation in the future when ResourceConfig is ready.
      // validate();

      return new ResourceConfig(_resourceId, _monitorDisabled, _numPartitions, _stateModelDefRef,
          _stateModelFactoryName, _numReplica, _minActiveReplica, _maxPartitionsPerInstance,
          _instanceGroupTag, _helixEnabled, _resourceGroupName, _resourceType, _groupRoutingEnabled,
          _externalViewDisabled, _rebalanceConfig, _stateTransitionTimeoutConfig, _preferenceLists,
          _mapFields, _p2pMessageEnabled);
    }
  }
}

