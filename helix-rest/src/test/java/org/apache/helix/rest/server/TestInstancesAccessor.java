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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.TestHelper;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestInstancesAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";
  private ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testInstancesStoppable_zoneBased() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Select instances with zone based
    String content =
        String.format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
            "instance2", "instance3", "instance4", "instance5", "invalidInstance");
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances?command=stoppable")
        .format(STOPPABLE_CLUSTER)
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(
        jsonNode.withArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name())
            .elements().hasNext());
    JsonNode nonStoppableInstances = jsonNode
        .get(InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance0"),
        ImmutableSet.of("Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance1"),
        ImmutableSet.of("Helix:EMPTY_RESOURCE_ASSIGNMENT", "Helix:INSTANCE_NOT_ENABLED",
            "Helix:INSTANCE_NOT_STABLE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance2"),
        ImmutableSet.of("Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance3"),
        ImmutableSet.of("Helix:HAS_DISABLED_PARTITION", "Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance4"),
        ImmutableSet.of("Helix:EMPTY_RESOURCE_ASSIGNMENT", "Helix:INSTANCE_NOT_ALIVE",
            "Helix:INSTANCE_NOT_STABLE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("Helix:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstancesStoppable_zoneBased")
  public void testInstancesStoppable_disableOneInstance() throws IOException {
    // Disable one selected instance0, it should failed to check
    String instance = "instance0";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER, instance);
    instanceConfig.setInstanceEnabled(false);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", false);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);

    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    Entity entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    JsonNode jsonResult = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(jsonResult.get("stoppable").asBoolean());
    Assert.assertEquals(getStringSet(jsonResult, "failedChecks"),
            ImmutableSet.of("Helix:HAS_DISABLED_PARTITION","Helix:INSTANCE_NOT_ENABLED","Helix:INSTANCE_NOT_STABLE","Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED"));

    // Reenable instance0, it should passed the check
    instanceConfig.setInstanceEnabled(true);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", true);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);
    Assert.assertTrue(verifier.verifyByPolling());

    entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);
    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    jsonResult = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Assert.assertFalse(jsonResult.get("stoppable").asBoolean());
    Assert.assertEquals(getStringSet(jsonResult, "failedChecks"), ImmutableSet.of("Helix:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstancesStoppable_disableOneInstance")
  public void testGetAllInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances").isBodyReturnExpected(true)
        .format(CLUSTER_NAME).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesStr =
        node.get(InstancesAccessor.InstancesProperties.instances.name()).toString();
    Assert.assertNotNull(instancesStr);

    Set<String> instances = OBJECT_MAPPER.readValue(instancesStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(instances, _instancesMap.get(CLUSTER_NAME), "Instances from response: "
        + instances + " vs instances actually: " + _instancesMap.get(CLUSTER_NAME));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(enabled = false)
  public void testUpdateInstances() throws IOException {
    // TODO: Reenable the test after storage node fix the problem
    // Batch disable instances

    List<String> instancesToDisable = Arrays.asList(new String[] {
        CLUSTER_NAME + "localhost_12918", CLUSTER_NAME + "localhost_12919",
        CLUSTER_NAME + "localhost_12920"
    });
    Entity entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap
            .of(InstancesAccessor.InstancesProperties.instances.name(), instancesToDisable)),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/instances", ImmutableMap.of("command", "disable"), entity,
        Response.Status.OK.getStatusCode());
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertEquals(clusterConfig.getDisabledInstances().keySet(),
        new HashSet<>(instancesToDisable));

    instancesToDisable = Arrays.asList(new String[] {
        CLUSTER_NAME + "localhost_12918", CLUSTER_NAME + "localhost_12920"
    });
    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap
            .of(InstancesAccessor.InstancesProperties.instances.name(), instancesToDisable)),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/instances", ImmutableMap.of("command", "enable"), entity,
        Response.Status.OK.getStatusCode());
    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertEquals(clusterConfig.getDisabledInstances().keySet(),
        new HashSet<>(Arrays.asList(CLUSTER_NAME + "localhost_12919")));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetAllInstances")
  public void testValidateWeightForAllInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Issue a validate call
    String body = new JerseyUriRequestBuilder("clusters/{}/instances?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    // Must have the results saying they are all valid (true) because there's no capacity keys set
    // in ClusterConfig
    node.iterator().forEachRemaining(child -> Assert.assertTrue(child.booleanValue()));

    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    body = new JerseyUriRequestBuilder("clusters/{}/instances?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode()).get(this);
    node = OBJECT_MAPPER.readTree(body);
    // Since instances do not have weight-related configs, the result should return error
    Assert.assertTrue(node.has("error"));

    // Now set weight-related configs in InstanceConfigs
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    for (String instance : instances) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setInstanceCapacityMap(ImmutableMap.of("FOO", 1000, "BAR", 1000));
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }

    body = new JerseyUriRequestBuilder("clusters/{}/instances?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME)
        .expectedReturnStatusCode(Response.Status.OK.getStatusCode()).get(this);
    node = OBJECT_MAPPER.readTree(body);
    // Must have the results saying they are all valid (true) because capacity keys are set
    // in ClusterConfig
    node.iterator().forEachRemaining(child -> Assert.assertTrue(child.booleanValue()));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private Set<String> getStringSet(JsonNode jsonNode, String key) {
    Set<String> result = new HashSet<>();
    jsonNode.withArray(key).forEach(s -> result.add(s.textValue()));
    return result;
  }
}
