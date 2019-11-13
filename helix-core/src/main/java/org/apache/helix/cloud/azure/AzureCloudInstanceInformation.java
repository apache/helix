package org.apache.helix.cloud.azure;

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

import java.util.Map;

import org.apache.helix.api.cloud.CloudInstanceInformation;


public class AzureCloudInstanceInformation implements CloudInstanceInformation {
  private Map<String, String> _cloudInstanceInfoMap;

  /**
   * Instantiate the AzureCloudInstanceInformation using each field individually.
   * Users should use AzureCloudInstanceInformation.Builder to create information.
   * @param cloudInstanceInfoMap
   */
  protected AzureCloudInstanceInformation(Map<String, String> cloudInstanceInfoMap) {
    _cloudInstanceInfoMap = cloudInstanceInfoMap;
  }

  @Override
  public String get(String key) {
    return _cloudInstanceInfoMap.get(key);
  }

  public static class Builder {
    private Map<String, String> _cloudInstanceInfoMap = null;

    public AzureCloudInstanceInformation build() {
      return new AzureCloudInstanceInformation(_cloudInstanceInfoMap);
    }

    /**
     * Default constructor
     */
    public Builder() {
    }

    public Builder setInstanceName(String v) {
      _cloudInstanceInfoMap.put(CloudInstanceField.INSTANCE_NAME.name(), v);
      return this;
    }

    public Builder setFaultDomain(String v) {
      _cloudInstanceInfoMap.put(CloudInstanceField.FAULT_DOMAIN.name(), v);
      return this;
    }

    public Builder setInstanceSetName(String v) {
      _cloudInstanceInfoMap.put(CloudInstanceField.INSTANCE_SET_NAME.name(), v);
      return this;
    }

    public Builder setCloudInstanceInfoField(String key, String value) {
      _cloudInstanceInfoMap.put(key, value);
      return this;
    }
  }
}