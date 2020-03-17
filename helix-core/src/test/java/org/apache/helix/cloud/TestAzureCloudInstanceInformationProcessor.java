package org.apache.helix.cloud;

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

import java.util.List;

import org.apache.helix.HelixCloudProperty;
import org.apache.helix.api.cloud.CloudInstanceInformation;
import org.apache.helix.cloud.azure.AzureCloudInstanceInformation;
import org.apache.helix.cloud.azure.AzureCloudInstanceInformationProcessor;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.model.CloudConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link AzureCloudInstanceInformationProcessor}
 */
public class TestAzureCloudInstanceInformationProcessor extends MockHttpClient {

  @Test()
  public void testAzureCloudInstanceInformationProcessing() throws Exception {
    String responseFile = "AzureResponse.json";

    CloudConfig.Builder cloudConfigBuilder = new CloudConfig.Builder();
    cloudConfigBuilder.setCloudEnabled(true);
    cloudConfigBuilder.setCloudProvider(CloudProvider.AZURE);
    cloudConfigBuilder.setCloudID("TestID");
    HelixCloudProperty helixCloudProperty = new HelixCloudProperty(cloudConfigBuilder.build());
    AzureCloudInstanceInformationProcessor processor = new AzureCloudInstanceInformationProcessor(
        helixCloudProperty, createMockHttpClient(responseFile));
    List<String> response = processor.fetchCloudInstanceInformation();

    Assert.assertEquals(response.size(), 1);
    Assert.assertNotNull(response.get(0));

    // Verify the response from mock http client
    AzureCloudInstanceInformation azureCloudInstanceInformation =
        processor.parseCloudInstanceInformation(response);
    Assert.assertEquals(
        azureCloudInstanceInformation
            .get(CloudInstanceInformation.CloudInstanceField.FAULT_DOMAIN.name()),
        "faultDomain=2," + "hostname=");
    Assert.assertEquals(azureCloudInstanceInformation
        .get(CloudInstanceInformation.CloudInstanceField.INSTANCE_SET_NAME.name()), "test-helix");
    Assert.assertEquals(
        azureCloudInstanceInformation
            .get(CloudInstanceInformation.CloudInstanceField.INSTANCE_NAME.name()),
        "d2b921cc-c16c-41f7-a86d-a445eac6ec26");
  }
}
