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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestTaskAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";

  @Test
  public void testGetAddTaskUserContent() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String uri = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/JOB0/tasks/0/userContent";
    String uriTaskDoesNotExist = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/JOB0/tasks/xxx/userContent";

    // Empty user content
    String body =
        get(uri, null, Response.Status.OK.getStatusCode(), true);
    Map<String, String>
        contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {});
    Assert.assertTrue(contentStore.isEmpty());

    // Post user content
    Map<String, String> map1 = new HashMap<>();
    map1.put("k1", "v1");
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(map1), MediaType.APPLICATION_JSON_TYPE);
    post(uri, ImmutableMap.of("command", "update"), entity, Response.Status.OK.getStatusCode());
    post(uriTaskDoesNotExist, ImmutableMap.of("command", "update"), entity, Response.Status.OK.getStatusCode());

    // get after post should work
    body = get(uri, null, Response.Status.OK.getStatusCode(), true);
    contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {
    });
    Assert.assertEquals(contentStore, map1);
    body = get(uriTaskDoesNotExist, null, Response.Status.OK.getStatusCode(), true);
    contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {
    });
    Assert.assertEquals(contentStore, map1);


    // modify map1 and verify
    map1.put("k1", "v2");
    map1.put("k2", "v2");
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(map1), MediaType.APPLICATION_JSON_TYPE);
    post(uri, ImmutableMap.of("command", "update"), entity, Response.Status.OK.getStatusCode());
    body = get(uri, null, Response.Status.OK.getStatusCode(), true);
    contentStore = OBJECT_MAPPER.readValue(body, new TypeReference<Map<String, String>>() {
    });
    Assert.assertEquals(contentStore, map1);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetAddTaskUserContent")
  public void testInvalidGetAddTaskUserContent() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String validURI = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/Job_0/tasks/0/userContent";
    String invalidURI1 = "clusters/" + CLUSTER_NAME + "/workflows/xxx/jobs/Job_0/tasks/0/userContent"; // workflow not exist
    String invalidURI2 = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/xxx/tasks/0/userContent"; // job not exist
    String invalidURI3 = "clusters/" + CLUSTER_NAME + "/workflows/Workflow_0/jobs/xxx/tasks/xxx/userContent"; // task not exist
    Entity validEntity = Entity.entity("{\"k1\":\"v1\"}", MediaType.APPLICATION_JSON_TYPE);
    Entity invalidEntity = Entity.entity("{\"k1\":{}}", MediaType.APPLICATION_JSON_TYPE); // not Map<String, String>
    Map<String, String> validCmd = ImmutableMap.of("command", "update");
    Map<String, String> invalidCmd = ImmutableMap.of("command", "delete"); // cmd not supported

    get(invalidURI1, null, Response.Status.NOT_FOUND.getStatusCode(), false);
    get(invalidURI2, null, Response.Status.NOT_FOUND.getStatusCode(), false);
    get(invalidURI3, null, Response.Status.NOT_FOUND.getStatusCode(), false);

    // The following two lines should get OK even though they should be NOT FOUND because the client
    // side code create UserContent znodes when not found
    post(invalidURI1, validCmd, validEntity, Response.Status.OK.getStatusCode());
    post(invalidURI2, validCmd, validEntity, Response.Status.OK.getStatusCode());

    post(validURI, invalidCmd, validEntity, Response.Status.BAD_REQUEST.getStatusCode());
    post(validURI, validCmd, invalidEntity, Response.Status.BAD_REQUEST.getStatusCode());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}
