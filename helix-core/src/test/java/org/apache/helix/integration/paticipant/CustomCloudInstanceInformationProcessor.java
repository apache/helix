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

import java.util.Collections;
import java.util.List;

import org.apache.helix.HelixCloudProperty;
import org.apache.helix.api.cloud.CloudInstanceInformation;
import org.apache.helix.api.cloud.CloudInstanceInformationProcessor;

/**
 * This is a custom implementation of CloudInstanceInformationProcessor.
 * It is used to test the functionality of Helix node auto-registration.
 */
public class CustomCloudInstanceInformationProcessor implements CloudInstanceInformationProcessor<String> {

  public CustomCloudInstanceInformationProcessor(HelixCloudProperty helixCloudProperty) {
  }

  @Override
  public List<String> fetchCloudInstanceInformation() {
    return Collections.singletonList("response");
  }

  @Override
  public CloudInstanceInformation parseCloudInstanceInformation(List<String> responses) {
    return new CustomCloudInstanceInformation();
  }
}