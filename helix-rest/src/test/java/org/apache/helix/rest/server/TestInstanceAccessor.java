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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.rest.server.resources.InstanceAccessor;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestInstanceAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";
  private final static String INSTANCE_NAME = CLUSTER_NAME + "localhost_12918";

  @Test
  public void testGetInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body =
        get("clusters/" + CLUSTER_NAME + "/instances", Response.Status.OK.getStatusCode(), true);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesStr =
        node.get(InstanceAccessor.InstanceProperties.instances.name()).toString();
    Assert.assertNotNull(instancesStr);

    Set<String> instances = OBJECT_MAPPER.readValue(instancesStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(instances, _instancesMap.get(CLUSTER_NAME),
        "Instances from response: " + instances + " vs instances actually: " + _instancesMap
            .get(CLUSTER_NAME));
  }

  @Test(dependsOnMethods = "testGetInstances")
  public void testGetInstance() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = get("clusters/" + CLUSTER_NAME + "/instances/" + INSTANCE_NAME,
        Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesCfg =
        node.get(InstanceAccessor.InstanceProperties.config.name()).toString();
    Assert.assertNotNull(instancesCfg);

    InstanceConfig instanceConfig = new InstanceConfig(toZNRecord(instancesCfg));
    Assert.assertEquals(instanceConfig,
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME));
  }

  @Test(dependsOnMethods = "updateInstance")
  public void testAddInstance() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE_NAME + "TEST");
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(instanceConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    put("clusters/" + CLUSTER_NAME + "/instances/" + INSTANCE_NAME, null, entity,
        Response.Status.OK.getStatusCode());
    Assert.assertEquals(instanceConfig,
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME + "TEST"));
  }

  @Test(dependsOnMethods = "testAddInstance", expectedExceptions = HelixException.class)
  public void testDeleteInstance() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    delete("clusters/" + CLUSTER_NAME + "/instances/" + INSTANCE_NAME + "TEST",
        Response.Status.OK.getStatusCode());
    _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME + "TEST");
  }

  @Test(dependsOnMethods = "testGetInstance")
  public void updateInstance() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Disable instance
    Entity entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/instances/" + INSTANCE_NAME,
        ImmutableMap.of("command", "disable"), entity, Response.Status.OK.getStatusCode());
    Assert.assertFalse(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());

    // Enable instance
    post("clusters/" + CLUSTER_NAME + "/instances/" + INSTANCE_NAME,
        ImmutableMap.of("command", "enable"), entity, Response.Status.OK.getStatusCode());
    Assert.assertTrue(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());

    // AddTags
    List<String> tagList = ImmutableList.of("tag3", "tag1", "tag2");
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(ImmutableMap
            .of(AbstractResource.Properties.id.name(), INSTANCE_NAME,
                InstanceAccessor.InstanceProperties.instanceTags.name(), tagList)),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/instances/" + INSTANCE_NAME,
        ImmutableMap.of("command", "addInstanceTag"), entity, Response.Status.OK.getStatusCode());
    Assert.assertEquals(_configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getTags(),
        tagList);

    // RemoveTags
    List<String> removeList = new ArrayList<>(tagList);
    removeList.remove("tag2");
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(ImmutableMap
            .of(AbstractResource.Properties.id.name(), INSTANCE_NAME,
                InstanceAccessor.InstanceProperties.instanceTags.name(), removeList)),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/instances/" + INSTANCE_NAME,
        ImmutableMap.of("command", "removeInstanceTag"), entity,
        Response.Status.OK.getStatusCode());
    Assert.assertEquals(_configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getTags(),
        ImmutableList.of("tag2"));
  }
}
