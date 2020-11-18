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
import java.util.List;

import org.apache.helix.api.cloud.CloudInstanceInformationProcessor;


public class AzureCloudInstanceInformationProcessor implements CloudInstanceInformationProcessor<String> {

  public AzureCloudInstanceInformationProcessor() {
  }

  /**
   * fetch the raw Azure cloud instance information
   * @return raw Azure cloud instance information
   */
  @Override
  public List<String> fetchCloudInstanceInformation() {
    List<String> response = new ArrayList<>();
    //TODO: implement the fetching logic
    return response;
  }

  /**
   * Parse raw Azure cloud instance information.
   * @return required azure cloud instance information
   */
  @Override
  public AzureCloudInstanceInformation parseCloudInstanceInformation(List<String> responses) {
    AzureCloudInstanceInformation azureCloudInstanceInformation = null;
    //TODO: implement the parsing logic
    return azureCloudInstanceInformation;
  }
}


