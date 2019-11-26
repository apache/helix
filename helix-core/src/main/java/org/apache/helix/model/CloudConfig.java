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

  public static final String CLOUD_CONFIG_KW = "CloudConfig";

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
   */
  private CloudConfig() {
    super(CLOUD_CONFIG_KW);
  }

  /**
   * The constructor from the ZNRecord.
   * @param record
   */
  private CloudConfig(ZNRecord record) {
    super(CLOUD_CONFIG_KW);
    _record.setSimpleFields(record.getSimpleFields());
    _record.setListFields(record.getListFields());
    _record.setMapFields(record.getMapFields());
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
   * Get the CLOUD_INFO_PROCESSOR_NAME field.
   * @return CLOUD_INFO_PROCESSOR_NAME field.
   */
  public String getCloudInfoProcessorName() {
    return _record.getSimpleField(CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name());
  }

  /**
   * Get the CLOUD_PROVIDER field.
   * @return CLOUD_PROVIDER field.
   */
  public String getCloudProvider() {
    return _record.getSimpleField(CloudConfigProperty.CLOUD_PROVIDER.name());
  }


  public static class Builder {
    private ZNRecord _record;

    public CloudConfig build() {
      validate();
      return new CloudConfig(_record);
    }

    /**
     * Default constructor
     */
    public Builder() {
      _record = new ZNRecord(CLOUD_CONFIG_KW);
    }

    /**
     * Instantiate with a pre-populated record
     * @param record a ZNRecord corresponding to a cloud configuration
     */
    public Builder(ZNRecord record) {
      _record = record;
    }

    /**
     * Constructor with CloudConfig as input
     * @param cloudConfig
     */
    public Builder(CloudConfig cloudConfig) {
      _record = cloudConfig.getRecord();
    }

    public Builder setCloudEnabled(boolean isEnabled) {
      _record.setBooleanField(CloudConfigProperty.CLOUD_ENABLED.name(), isEnabled);
      return this;
    }

    public Builder setCloudProvider(CloudProvider cloudProvider) {
      _record.setSimpleField(CloudConfigProperty.CLOUD_PROVIDER.name(), cloudProvider.name());
      return this;
    }

    public Builder setCloudID(String cloudID) {
      _record.setSimpleField(CloudConfigProperty.CLOUD_ID.name(), cloudID);
      return this;
    }

    public Builder setCloudInfoSources(List<String> cloudInfoSources) {
      _record.setListField(CloudConfigProperty.CLOUD_INFO_SOURCE.name(), cloudInfoSources);
      return this;
    }

    public Builder addCloudInfoSource(String cloudInfoSource) {
      if (_record.getListField(CloudConfigProperty.CLOUD_INFO_SOURCE.name()) == null) {
        _record.setListField(CloudConfigProperty.CLOUD_INFO_SOURCE.name(), new ArrayList<String>());
      }
      List<String> cloudInfoSourcesList = _record.getListField(CloudConfigProperty.CLOUD_INFO_SOURCE.name());
      cloudInfoSourcesList.add(cloudInfoSource);
      _record.setListField(CloudConfigProperty.CLOUD_INFO_SOURCE.name(), cloudInfoSourcesList);
      return this;
    }

    public Builder setCloudInfoProcessorName(String cloudInfoProcessorName) {
      _record.setSimpleField(CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(),
          cloudInfoProcessorName);
      return this;
    }

    public String getCloudProvider() {
      return _record.getSimpleField(CloudConfigProperty.CLOUD_PROVIDER.name());
    }

    public boolean getCloudEnabled() {
      return _record.getBooleanField(CloudConfigProperty.CLOUD_ENABLED.name(),
          DEFAULT_CLOUD_ENABLED);
    }

    public String getCloudID() {
      return _record.getSimpleField(CloudConfigProperty.CLOUD_ID.name());
    }

    public List<String> getCloudInfoSources() {
      return _record.getListField(CloudConfigProperty.CLOUD_INFO_SOURCE.name());
    }

    public String getCloudInfoProcessorName() {
      return _record.getSimpleField(CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name());
    }

    private void validate() {
      if (this.getCloudProvider() == null) {
        throw new HelixException(
            "This Cloud Configuration is Invalid. The Cloud Provider is missing from the config.");
      } else if (this.getCloudProvider().equals(CloudProvider.CUSTOMIZED.name())) {
        if (this.getCloudInfoProcessorName() == null || this.getCloudInfoSources() == null
            || this.getCloudInfoSources().size() == 0) {
          throw new HelixException(
              "This Cloud Configuration is Invalid. CUSTOMIZED provider has been chosen without defining CloudInfoProcessorName or CloudInfoSources");
        }
      }
    }
  }
}
