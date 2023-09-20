package org.apache.helix.integration.paticipant;

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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.api.cloud.CloudInstanceInformation;
import org.apache.helix.api.cloud.CloudInstanceInformationV2;

/**
 * This is a custom implementation of CloudInstanceInformation. It is used to test the functionality
 * of Helix node auto-registration.
 */
public class CustomCloudInstanceInformation implements CloudInstanceInformationV2 {

  public static final Map<String, String> _cloudInstanceInfo =
      ImmutableMap.of(CloudInstanceInformation.CloudInstanceField.FAULT_DOMAIN.name(),
          "mz=0, host=localhost, container=containerId", "MAINTENANCE_ZONE", "0", "INSTANCE_NAME",
          "localhost_something");
  ;

  public CustomCloudInstanceInformation() {
  }

  @Override
  public String get(String key) {
    return _cloudInstanceInfo.get(key);
  }

  @Override
  public Map<String, String> getAll() {
    return _cloudInstanceInfo;
  }
}
