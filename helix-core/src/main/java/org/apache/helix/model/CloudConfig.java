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
import java.util.List;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.cloud.constants.CloudProvider;

/**
 * Cloud configurations
 */
public class CloudConfig extends HelixProperty {
  /**
   * Configurable characteristics of a cloud.
   * NOTE: Do NOT use this field name directly, use its corresponding getter/setter in the
   * CloudConfig.
   */
  public enum CloudConfigProperty {
    CLOUD_ENABLED, // determine whether the cluster is inside cloud environment.
    CLOUD_PROVIDER, // the environment the cluster is in, e.g. Azure, AWS, or Customized
    CLOUD_ID, // the cloud Id that belongs to this cluster.

    // If user uses Helix supported default provider, the below entries will not be shown in
    // CloudConfig.
    CLOUD_INFO_SOURCE, // the source for retrieving the cloud information.
    CLOUD_INFO_PROCESSOR_NAME // the name of the function that processes the fetching and parsing of
                              // cloud information.
  }

  /* Default values */
  private static final boolean DEFAULT_CLOUD_ENABLED = false;

  /**
   * Instantiate the CloudConfig for the cloud
   * @param cluster
   */
  public CloudConfig(String cluster) {
    super(cluster);
  }

  /**
   * Instantiate with a pre-populated record
   * @param record a ZNRecord corresponding to a cloud configuration
   */
  public CloudConfig(ZNRecord record) {
    super(record);
  }

  /**
   * Instantiate the config using each field individually.
   * Users should use CloudConfig.Builder to create CloudConfig.
   * @param cluster
   * @param enabled
   * @param cloudID
   */
  public CloudConfig(String cluster, boolean enabled, CloudProvider cloudProvider, String cloudID,
      List<String> cloudInfoSource, String cloudProcessorName) {
    super(cluster);
    _record.setBooleanField(CloudConfigProperty.CLOUD_ENABLED.name(), enabled);
    _record.setSimpleField(CloudConfigProperty.CLOUD_PROVIDER.name(), cloudProvider.name());
    _record.setSimpleField(CloudConfigProperty.CLOUD_ID.name(), cloudID);
    if (cloudProvider.equals(CloudProvider.CUSTOMIZED)) {
      _record
          .setSimpleField(CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(), cloudProcessorName);
      _record.setListField(CloudConfigProperty.CLOUD_INFO_SOURCE.name(), cloudInfoSource);
    }
  }

  /**
   * Enable/Disable the CLOUD_ENABLED field.
   * @param enabled
   */
  public void setCloudEnabled(boolean enabled) {
    _record.setBooleanField(CloudConfigProperty.CLOUD_ENABLED.name(), enabled);
  }

  /**
   * Whether CLOUD_ENABLED field is enabled or not.
   * @return
   */
  public boolean isCloudEnabled() {
    return _record.getBooleanField(CloudConfigProperty.CLOUD_ENABLED.name(), false);
  }

  /**
   * Set the cloudID field.
   * @param cloudID
   */
  public void setCloudID(String cloudID) {
    _record.setSimpleField(CloudConfigProperty.CLOUD_ID.name(), cloudID);
  }

  /**
   * Get the CloudID field.
   * @return CloudID
   */
  public String getCloudID() {
    return _record.getSimpleField(CloudConfigProperty.CLOUD_ID.name());
  }

  /**
   * Set the CLOUD_INFO_SOURCE field.
   * @param cloudInfoSources
   */
  public void setCloudInfoSource(List<String> cloudInfoSources) {
    _record.setListField(CloudConfigProperty.CLOUD_INFO_SOURCE.name(), cloudInfoSources);
  }

  /**
   * Get the CLOUD_INFO_SOURCE field.
   * @return CLOUD_INFO_SOURCE field.
   */
  public List<String> getCloudInfoSources() {
    return _record.getListField(CloudConfigProperty.CLOUD_INFO_SOURCE.name());
  }

  /**
   * Set the CLOUD_INFO_PROCESSOR_NAME field.
   * @param cloudInfoProcessorName
   */
  public void setCloudInfoFProcessorName(String cloudInfoProcessorName) {
    _record.setSimpleField(CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(),
        cloudInfoProcessorName);
  }

  /**
   * Get the CLOUD_INFO_PROCESSOR_NAME field.
   * @return CLOUD_INFO_PROCESSOR_NAME field.
   */
  public String getCloudInfoProcessorName() {
    return _record.getSimpleField(CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name());
  }

  /**
   * Set the CLOUD_PROVIDER field.
   * @param cloudProvider
   */
  public void setCloudProvider(CloudProvider cloudProvider) {
    _record.setSimpleField(CloudConfigProperty.CLOUD_PROVIDER.name(), cloudProvider.name());
  }

  /**
   * Get the CLOUD_PROVIDER field.
   * @return CLOUD_PROVIDER field.
   */
  public String getCloudProvider() {
    return _record.getSimpleField(CloudConfigProperty.CLOUD_PROVIDER.name());
  }

  public static class Builder {
    private String _clusterName = null;
    private CloudProvider _cloudProvider;
    private boolean _cloudEnabled = DEFAULT_CLOUD_ENABLED;
    private String _cloudID;
    private List<String> _cloudInfoSources;
    private String _cloudInfoProcessorName;

    public CloudConfig build() {
      validate();
      return new CloudConfig(_clusterName, _cloudEnabled, _cloudProvider, _cloudID,
          _cloudInfoSources, _cloudInfoProcessorName);
    }

    /**
     * Default constructor
     */
    public Builder() {
    }

    /**
     * Constructor with Cluster Name as input
     * @param clusterName
     */
    public Builder(String clusterName) {
      _clusterName = clusterName;
    }

    /**
     * Constructor with CloudConfig as input
     * @param cloudConfig
     */
    public Builder(CloudConfig cloudConfig) {
      _cloudEnabled = cloudConfig.isCloudEnabled();
      _cloudProvider = CloudProvider.valueOf(cloudConfig.getCloudProvider());
      _cloudID = cloudConfig.getCloudID();
      _cloudInfoSources = cloudConfig.getCloudInfoSources();
      _cloudInfoProcessorName = cloudConfig.getCloudInfoProcessorName();
    }

    public Builder setClusterName(String v) {
      _clusterName = v;
      return this;
    }

    public Builder setCloudEnabled(boolean isEnabled) {
      _cloudEnabled = isEnabled;
      return this;
    }

    public Builder setCloudProvider(CloudProvider cloudProvider) {
      _cloudProvider = cloudProvider;
      return this;
    }

    public Builder setCloudID(String v) {
      _cloudID = v;
      return this;
    }

    public Builder setCloudInfoSources(List<String> v) {
      _cloudInfoSources = v;
      return this;
    }

    public Builder addCloudInfoSource(String v) {
      if (_cloudInfoSources == null) {
        _cloudInfoSources = new ArrayList<String>();
      }
      _cloudInfoSources.add(v);
      return this;
    }

    public Builder setCloudInfoProcessorName(String v) {
      _cloudInfoProcessorName = v;
      return this;
    }

    public String getClusterName() {
      return _clusterName;
    }

    public CloudProvider getCloudProvider() {
      return _cloudProvider;
    }

    public boolean getCloudEnabled() {
      return _cloudEnabled;
    }

    public String getCloudID() {
      return _cloudID;
    }

    public List<String> getCloudInfoSources() {
      return _cloudInfoSources;
    }

    public String getCloudInfoProcessorName() {
      return _cloudInfoProcessorName;
    }

    private void validate() {
      if (_cloudEnabled) {
        if (_cloudID == null) {
          throw new HelixException(
              "This Cloud Configuration is Invalid. The CloudID is missing from the config.");
        }
        if (_cloudProvider == null) {
          throw new HelixException(
              "This Cloud Configuration is Invalid. The Cloud Provider is missing from the config.");
        } else if (_cloudProvider == CloudProvider.CUSTOMIZED) {
          if (_cloudInfoProcessorName == null || _cloudInfoSources == null || _cloudInfoSources.size() == 0) {
            throw new HelixException(
                "This Cloud Configuration is Invalid. CUSTOMIZED provider has been chosen without defining CloudInfoProcessorName or CloudInfoSources");
          }
        }
      }
    }
  }
}
