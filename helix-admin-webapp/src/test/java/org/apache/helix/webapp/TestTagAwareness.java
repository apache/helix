package org.apache.helix.webapp;

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
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.AdminTestBase;
import org.apache.helix.webapp.resources.ClusterRepresentationUtil;
import org.apache.helix.webapp.resources.InstancesResource.ListInstancesWrapper;
import org.junit.Assert;
import org.restlet.Client;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Ensure that REST calls for participants and resources return information about tags
 */
public class TestTagAwareness extends AdminTestBase {
  private String _clusterName;
  private HelixAdmin _admin;

  @BeforeClass
  public void beforeClass() {
    _clusterName = TestTagAwareness.class.getCanonicalName() + "_cluster";
    _gSetupTool.addCluster(_clusterName, true);
    _admin = _gSetupTool.getClusterManagementTool();
  }

  @Test
  public void testGetResources() throws IOException {
    final String TAG = "tag";
    final String URL_BASE =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + _clusterName + "/resourceGroups";

    // Add a tagged resource
    IdealState taggedResource = new IdealState("taggedResource");
    taggedResource.setInstanceGroupTag(TAG);
    taggedResource.setStateModelDefRef("OnlineOffline");
    _admin.addResource(_clusterName, taggedResource.getId(), taggedResource);

    // Add an untagged resource
    IdealState untaggedResource = new IdealState("untaggedResource");
    untaggedResource.setStateModelDefRef("OnlineOffline");
    _admin.addResource(_clusterName, untaggedResource.getId(), untaggedResource);

    // Now make a REST call for all resources
    Reference resourceRef = new Reference(URL_BASE);
    Request request = new Request(Method.GET, resourceRef);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);
    ZNRecord responseRecord =
        ClusterRepresentationUtil.JsonToObject(ZNRecord.class, response.getEntityAsText());

    // Ensure that the tagged resource has information and the untagged one doesn't
    Assert.assertNotNull(responseRecord.getMapField("ResourceTags"));
    Assert
        .assertEquals(TAG, responseRecord.getMapField("ResourceTags").get(taggedResource.getId()));
    Assert.assertFalse(responseRecord.getMapField("ResourceTags").containsKey(
        untaggedResource.getId()));
  }

  @Test
  public void testGetInstances() throws IOException {
    final String[] TAGS = {
        "tag1", "tag2"
    };
    final String URL_BASE =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + _clusterName + "/instances";

    // Add 4 participants, each with differint tag characteristics
    InstanceConfig instance1 = new InstanceConfig("localhost_1");
    instance1.addTag(TAGS[0]);
    _admin.addInstance(_clusterName, instance1);
    InstanceConfig instance2 = new InstanceConfig("localhost_2");
    instance2.addTag(TAGS[1]);
    _admin.addInstance(_clusterName, instance2);
    InstanceConfig instance3 = new InstanceConfig("localhost_3");
    instance3.addTag(TAGS[0]);
    instance3.addTag(TAGS[1]);
    _admin.addInstance(_clusterName, instance3);
    InstanceConfig instance4 = new InstanceConfig("localhost_4");
    _admin.addInstance(_clusterName, instance4);

    // Now make a REST call for all resources
    Reference resourceRef = new Reference(URL_BASE);
    Request request = new Request(Method.GET, resourceRef);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);
    ListInstancesWrapper responseWrapper =
        ClusterRepresentationUtil.JsonToObject(ListInstancesWrapper.class,
            response.getEntityAsText());
    Map<String, List<String>> tagInfo = responseWrapper.tagInfo;

    // Ensure tag ownership is reported correctly
    Assert.assertTrue(tagInfo.containsKey(TAGS[0]));
    Assert.assertTrue(tagInfo.containsKey(TAGS[1]));
    Assert.assertTrue(tagInfo.get(TAGS[0]).contains("localhost_1"));
    Assert.assertFalse(tagInfo.get(TAGS[0]).contains("localhost_2"));
    Assert.assertTrue(tagInfo.get(TAGS[0]).contains("localhost_3"));
    Assert.assertFalse(tagInfo.get(TAGS[0]).contains("localhost_4"));
    Assert.assertFalse(tagInfo.get(TAGS[1]).contains("localhost_1"));
    Assert.assertTrue(tagInfo.get(TAGS[1]).contains("localhost_2"));
    Assert.assertTrue(tagInfo.get(TAGS[1]).contains("localhost_3"));
    Assert.assertFalse(tagInfo.get(TAGS[1]).contains("localhost_4"));
  }

  @AfterClass
  public void afterClass() {
    _admin.dropCluster(_clusterName);
  }

}
