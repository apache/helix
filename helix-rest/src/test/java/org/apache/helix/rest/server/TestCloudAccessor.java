package org.apache.helix.rest.server;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCloudAccessor extends AbstractTestClass {

  @Test
  public void testAddCloudConfigNonExistedCluster() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String urlBase = "clusters/TestCloud/cloudconfig/";
    ZNRecord record = new ZNRecord("TestCloud");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.AZURE.name());
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    List<String> testList = new ArrayList<String>();
    testList.add("TestURL");
    record.setListField(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE.name(), testList);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(),
        "TestProcessor");

    // Not found since the cluster is not setup yet.
    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.NOT_FOUND.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddCloudConfigNonExistedCluster")
  public void testAddCloudConfig() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _gSetupTool.addCluster("TestCloud", true);
    String urlBase = "clusters/TestCloud/cloudconfig/";

    ZNRecord record = new ZNRecord("TestCloud");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.CUSTOMIZED.name());
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    List<String> testList = new ArrayList<String>();
    testList.add("TestURL");
    record.setListField(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE.name(), testList);

    // Bad request since Processor has not been defined.
    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());

    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(),
        "TestProcessorName");

    // Now response should be OK since all fields are set
    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig("TestCloud");
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudID(), "TestCloudID");
    List<String> listUrlFromZk = cloudConfigFromZk.getCloudInfoSources();
    Assert.assertEquals(listUrlFromZk.get(0), "TestURL");
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoProcessorName(), "TestProcessorName");
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.CUSTOMIZED.name());

    // Now test the getCloudConfig method.
    String body = get(urlBase, null, Response.Status.OK.getStatusCode(), true);

    ZNRecord recordFromRest = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    CloudConfig cloudConfigRest = new CloudConfig.Builder(recordFromRest).build();
    CloudConfig cloudConfigZk = _configAccessor.getCloudConfig("TestCloud");

    // Check that the CloudConfig from Zk and REST get method are equal
    Assert.assertEquals(cloudConfigRest, cloudConfigZk);

    // Check the fields individually
    Assert.assertTrue(cloudConfigRest.isCloudEnabled());
    Assert.assertEquals(cloudConfigRest.getCloudID(), "TestCloudID");
    Assert.assertEquals(cloudConfigRest.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    List<String> listUrlFromRest = cloudConfigRest.getCloudInfoSources();
    Assert.assertEquals(listUrlFromRest.get(0), "TestURL");
    Assert.assertEquals(cloudConfigRest.getCloudInfoProcessorName(), "TestProcessorName");

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddCloudConfig")
  public void testDeleteCloudConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _gSetupTool.addCluster("TestCloud", true);
    String urlBase = "clusters/TestCloud/cloudconfig/";

    Map<String, String> map1 = new HashMap<>();
    map1.put("command", AbstractResource.Command.delete.name());

    post(urlBase, map1, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Read CloudConfig from Zookeeper and make sure it has been removed
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig("TestCloud");
    Assert.assertNull(cloudConfigFromZk);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testDeleteCloudConfig")
  public void testUpdateCloudConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _gSetupTool.addCluster("TestCloud", true);
    String urlBase = "clusters/TestCloud/cloudconfig/";

    ZNRecord record = new ZNRecord("TestCloud");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudID");
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.AZURE.name());

    // Fist add CloudConfig to the cluster
    put(urlBase, null,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Now get the Cloud Config and make sure the information is correct
    String body = get(urlBase, null, Response.Status.OK.getStatusCode(), true);

    ZNRecord recordFromRest = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    CloudConfig cloudConfigRest = new CloudConfig.Builder(recordFromRest).build();
    Assert.assertTrue(cloudConfigRest.isCloudEnabled());
    Assert.assertEquals(cloudConfigRest.getCloudID(), "TestCloudID");
    Assert.assertEquals(cloudConfigRest.getCloudProvider(), CloudProvider.AZURE.name());

    // Now put new information in the ZNRecord
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.CUSTOMIZED.name());
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_ID.name(), "TestCloudIdNew");
    List<String> testList = new ArrayList<String>();
    testList.add("TestURL");
    record.setListField(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE.name(), testList);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME.name(),
        "TestProcessorName");

    Map<String, String> map1 = new HashMap<>();
    map1.put("command", AbstractResource.Command.update.name());

    post(urlBase, map1,
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    // Now get the Cloud Config and make sure the information has been updated
    body = get(urlBase, null, Response.Status.OK.getStatusCode(), true);

    recordFromRest = new ObjectMapper().reader(ZNRecord.class).readValue(body);
    cloudConfigRest = new CloudConfig.Builder(recordFromRest).build();
    Assert.assertTrue(cloudConfigRest.isCloudEnabled());
    Assert.assertEquals(cloudConfigRest.getCloudID(), "TestCloudIdNew");
    Assert.assertEquals(cloudConfigRest.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    List<String> listUrlFromRest = cloudConfigRest.getCloudInfoSources();
    Assert.assertEquals(listUrlFromRest.get(0), "TestURL");
    Assert.assertEquals(cloudConfigRest.getCloudInfoProcessorName(), "TestProcessorName");

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}
