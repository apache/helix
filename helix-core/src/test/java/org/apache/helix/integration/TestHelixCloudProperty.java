package org.apache.helix.integration;

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

import org.apache.helix.HelixCloudProperty;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.model.CloudConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixCloudProperty {
  @Test
  public void testHelixCloudPropertyAzure() {
    CloudConfig azureCloudConfig =
        new CloudConfig("cluster_foo", true, CloudProvider.AZURE, "azure1",
            Collections.singletonList("foo"), "foo");
    HelixCloudProperty azureCloudProperty = new HelixCloudProperty(azureCloudConfig);

    Assert.assertTrue(azureCloudProperty.getCloudEnabled());
    Assert.assertEquals(azureCloudProperty.getCloudId(), "azure1");
    Assert.assertEquals(azureCloudProperty.getCloudProvider(), CloudProvider.AZURE.name());
    Assert.assertEquals(azureCloudProperty.getCloudInfoSources(), Collections.singletonList(
        "http://169.254.169.254/metadata/instance?api-version=2019-06-04"));
    Assert.assertEquals(azureCloudProperty.getCloudMaxRetry(), 5);
    Assert.assertEquals(azureCloudProperty.getCloudConnectionTimeout(), 5000);
    Assert.assertEquals(azureCloudProperty.getCloudRequestTimeout(), 5000);
    Assert.assertEquals(azureCloudProperty.getCloudInfoProcessorFullyQualifiedClassName(),
        "org.apache.helix.cloud.azure.AzureCloudInstanceInformationProcessor");
  }

  @Test
  public void testHelixCloudPropertyCustomizedFullyQualified() {
    CloudConfig customCloudConfig =
        new CloudConfig("cluster_foo", true, CloudProvider.CUSTOMIZED, "custom1",
            Collections.singletonList("https://custom-cloud.com"),
            "com.linkedin.cloudinfo.CustomCloudInstanceInfoProcessor");
    HelixCloudProperty customCloudProperty = new HelixCloudProperty(customCloudConfig);

    Assert.assertTrue(customCloudProperty.getCloudEnabled());
    Assert.assertEquals(customCloudProperty.getCloudId(), "custom1");
    Assert.assertEquals(customCloudProperty.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    Assert.assertEquals(customCloudProperty.getCloudInfoSources(),
        Collections.singletonList("https://custom-cloud.com"));
    Assert.assertEquals(customCloudProperty.getCloudInfoProcessorFullyQualifiedClassName(),
        "com.linkedin.cloudinfo.CustomCloudInstanceInfoProcessor");
  }

  @Test
  public void testHelixCloudPropertyClassName() {
    CloudConfig customCloudConfig =
        new CloudConfig("cluster_foo", true, CloudProvider.CUSTOMIZED, "custom1",
            Collections.singletonList("https://custom-cloud.com"),
            "CustomCloudInstanceInfoProcessor");
    HelixCloudProperty customCloudProperty = new HelixCloudProperty(customCloudConfig);

    Assert.assertTrue(customCloudProperty.getCloudEnabled());
    Assert.assertEquals(customCloudProperty.getCloudId(), "custom1");
    Assert.assertEquals(customCloudProperty.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    Assert.assertEquals(customCloudProperty.getCloudInfoSources(),
        Collections.singletonList("https://custom-cloud.com"));
    Assert.assertEquals(customCloudProperty.getCloudInfoProcessorFullyQualifiedClassName(),
        "org.apache.helix.cloud.customized.CustomCloudInstanceInfoProcessor");
  }
}
