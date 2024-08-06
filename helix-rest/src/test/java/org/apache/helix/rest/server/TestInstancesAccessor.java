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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestInstancesAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_4";

  @DataProvider
  public Object[][] generatePayloadCrossZoneStoppableCheckWithZoneOrder() {
    return new Object[][]{
        {String.format(
            "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\","
                + " \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\"],"
                + "\"%s\":[\"%s\", \"%s\", \"%s\", \"%s\", \"%s\"], \"%s\":[\"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance2",
            "instance3", "instance4", "instance5", "instance6", "instance7", "instance8",
            "instance9", "instance10", "instance11", "instance12", "instance13", "instance14",
            "invalidInstance",
            InstancesAccessor.InstancesProperties.zone_order.name(),"zone5", "zone4", "zone3", "zone2",
            "zone1",
            InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(),
            "instance0"),
        },
        {String.format(
            "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"],"
                + "\"%s\":[\"%s\", \"%s\", \"%s\", \"%s\", \"%s\"], \"%s\":[\"%s\", \"%s\", \"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
            "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
            "instance14", "invalidInstance",
            InstancesAccessor.InstancesProperties.zone_order.name(), "zone5", "zone4", "zone1",
            "zone3", "zone2", InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(),
            "instance0", "invalidInstance1", "invalidInstance1"),
        }
    };
  }

  @Test
  public void testInstanceStoppableZoneBasedWithToBeStoppedInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"], \"%s\":[\"%s\",\"%s\"], \"%s\":[\"%s\", \"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1",
        "instance2", "instance3", "instance4", "instance5", "invalidInstance",
        InstancesAccessor.InstancesProperties.zone_order.name(), "zone2", "zone1",
        InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(), "instance0", "instance6", "invalidInstance1");

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance4") && stoppableSet.contains("instance3"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    //  "StoppableTestCluster2_db_0_3" : { "instance0" : "MASTER", "instance13" : "SLAVE", "instance5" : "SLAVE"}.
    //  Since instance0 is to_be_stopped and MIN_ACTIVE_REPLICA is 2, instance5 is not stoppable.
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance5"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstanceStoppableZoneBasedWithToBeStoppedInstances")
  public void testInstanceStoppableZoneBasedWithoutZoneOrder() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"], \"%s\":[\"%s\", \"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
        "instance2", "instance3", "instance4", "invalidInstance",
        InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(),
        "instance7", "instance9", "instance10");

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    // Without zone order, helix should pick the zone1 because it has higher instance count than zone2.
    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance0") && stoppableSet.contains("instance1"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance2"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dataProvider = "generatePayloadCrossZoneStoppableCheckWithZoneOrder",
      dependsOnMethods = "testInstanceStoppableZoneBasedWithoutZoneOrder")
  public void testCrossZoneStoppableWithZoneOrder(String content) throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance14") && stoppableSet.contains("instance12")
        && stoppableSet.contains("instance11") && stoppableSet.contains("instance10"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance13"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testCrossZoneStoppableWithZoneOrder")
  public void testCrossZoneStoppableWithoutZoneOrder() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"],"
            + "\"%s\":[\"%s\", \"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
        "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
        "instance14", "invalidInstance",
        InstancesAccessor.InstancesProperties.to_be_stopped_instances.name(), "instance0",
        "invalidInstance1", "invalidInstance1");

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance14") && stoppableSet.contains("instance12")
        && stoppableSet.contains("instance11") && stoppableSet.contains("instance10"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance13"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testCrossZoneStoppableWithoutZoneOrder")
  public void testInstanceStoppableCrossZoneBasedWithSelectedCheckList() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Select instances with cross zone based and perform all checks
    String content =
        String.format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"], \"%s\":[\"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
            "instance2", "instance3", "instance4", "instance5", "invalidInstance",
            InstancesAccessor.InstancesProperties.skip_stoppable_check_list.name(), "DUMMY_TEST_NO_EXISTS");

    new JerseyUriRequestBuilder("clusters/{}/instances?command=stoppable").format(STOPPABLE_CLUSTER)
        .isBodyReturnExpected(true)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode())
        .post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));

    // Select instances with cross zone based and perform a subset of checks
    content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"], \"%s\":[\"%s\",\"%s\"], \"%s\":[\"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
        "instance2", "instance3", "instance4", "instance5", "invalidInstance",
        InstancesAccessor.InstancesProperties.zone_order.name(), "zone2", "zone1",
        InstancesAccessor.InstancesProperties.skip_stoppable_check_list.name(), "INSTANCE_NOT_ENABLED", "INSTANCE_NOT_STABLE");
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance5"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ALIVE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance4"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ALIVE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance1"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstanceStoppableCrossZoneBasedWithSelectedCheckList")
  public void testInstanceStoppableCrossZoneBasedWithEvacuatingInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\", \"%s\", \"%s\", \"%s\",\"%s\", \"%s\", \"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.cross_zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance1", "instance3",
        "instance6", "instance9", "instance10", "instance11", "instance12", "instance13",
        "instance14", "invalidInstance");

    // Change instance config of instance1 & instance0 to be evacuating
    String instance0 = "instance0";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER2, instance0);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance0, instanceConfig);
    String instance1 = "instance1";
    InstanceConfig instanceConfig1 = _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER2, instance1);
    instanceConfig1.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance1, instanceConfig1);
    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER2).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(stoppableSet.contains("instance12")
        && stoppableSet.contains("instance11") && stoppableSet.contains("instance10"));

    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance13"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance14"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance0, instanceConfig);
    instanceConfig1.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER2, instance1, instanceConfig1);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstanceStoppableCrossZoneBasedWithEvacuatingInstances")
  public void testInstanceStoppable_zoneBased_zoneOrder() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Select instances with zone based
    String content = String.format(
        "{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"], \"%s\":[\"%s\",\"%s\"]}",
        InstancesAccessor.InstancesProperties.selection_base.name(),
        InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
        InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
        "instance2", "instance3", "instance4", "instance5", "invalidInstance",
        InstancesAccessor.InstancesProperties.zone_order.name(), "zone2", "zone1");
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(
        jsonNode.withArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name())
            .elements().hasNext());
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance5"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ALIVE",
            "HELIX:INSTANCE_NOT_STABLE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"),
        ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstanceStoppable_zoneBased_zoneOrder")
  public void testInstancesStoppable_zoneBased() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Select instances with zone based
    String content =
        String.format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\", \"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
            "instance2", "instance3", "instance4", "instance5", "invalidInstance");
    Response response =
        new JerseyUriRequestBuilder("clusters/{}/instances?command=stoppable").format(
            STOPPABLE_CLUSTER).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(
        jsonNode.withArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name())
            .elements().hasNext());
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance0"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance1"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance2"),
        ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance3"),
        ImmutableSet.of("HELIX:HAS_DISABLED_PARTITION", "HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "instance4"),
        ImmutableSet.of("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ALIVE",
            "HELIX:INSTANCE_NOT_STABLE"));
    Assert.assertEquals(getStringSet(nonStoppableInstances, "invalidInstance"), ImmutableSet.of("HELIX:INSTANCE_NOT_EXIST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testInstancesStoppable_zoneBased")
  public void testInstancesStoppable_disableOneInstance() throws IOException {
    // Disable one selected instance0, it should failed to check
    String instance = "instance0";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(STOPPABLE_CLUSTER, instance);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.DISABLE);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", false);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);

    // It takes time to reflect the changes.
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    Entity entity = Entity.entity("\"{}\"", MediaType.APPLICATION_JSON_TYPE);
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    JsonNode jsonResult = OBJECT_MAPPER.readTree(response.readEntity(String.class));
    Assert.assertFalse(jsonResult.get("stoppable").asBoolean());
    Assert.assertEquals(getStringSet(jsonResult, "failedChecks"),
            ImmutableSet.of("HELIX:HAS_DISABLED_PARTITION","HELIX:INSTANCE_NOT_ENABLED","HELIX:INSTANCE_NOT_STABLE","HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));

    // Reenable instance0, it should passed the check
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    instanceConfig.setInstanceEnabledForPartition("FakeResource", "FakePartition", true);
    _configAccessor.setInstanceConfig(STOPPABLE_CLUSTER, instance, instanceConfig);
    Assert.assertTrue(verifier.verifyByPolling());

    entity = Entity.entity("\"{}\"", MediaType.APPLICATION_JSON_TYPE);
    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, instance).post(this, entity);
    jsonResult = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    Assert.assertFalse(jsonResult.get("stoppable").asBoolean());
    Assert.assertEquals(getStringSet(jsonResult, "failedChecks"), ImmutableSet.of("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
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
    Assert.assertEquals(instances.size(), _instancesMap.get(CLUSTER_NAME).size(), "Different amount of elements in "
        + "the sets: " + instances.size() + " vs: " + _instancesMap.get(CLUSTER_NAME).size());
    Assert.assertTrue(instances.containsAll(_instancesMap.get(CLUSTER_NAME)), "instances set does not contain all "
        + "elements of _instanceMap");
    Assert.assertTrue(_instancesMap.get(CLUSTER_NAME).containsAll(instances), "_instanceMap set does not contain all "
        + "elements of instances");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(enabled = false)
  public void testUpdateInstances() throws IOException {
    // TODO: Reenable the test after storage node fix the problem
    // Batch disable instances

    List<String> instancesToDisable = Arrays.asList(new String[]{
        CLUSTER_NAME + "localhost_12918",
        CLUSTER_NAME + "localhost_12919", CLUSTER_NAME + "localhost_12920"});
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(ImmutableMap
            .of(InstancesAccessor.InstancesProperties.instances.name(), instancesToDisable)),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/instances", ImmutableMap
        .of("command", "disable", "instanceDisabledType", "USER_OPERATION",
            "instanceDisabledReason", "reason_1"), entity, Response.Status.OK.getStatusCode());
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertEquals(clusterConfig.getDisabledInstances().keySet(),
        new HashSet<>(instancesToDisable));
    Assert.assertEquals(clusterConfig.getDisabledInstancesWithInfo().keySet(),
        new HashSet<>(instancesToDisable));
    Assert
        .assertEquals(clusterConfig.getInstanceHelixDisabledType(CLUSTER_NAME + "localhost_12918"),
            "USER_OPERATION");
    Assert.assertEquals(
        clusterConfig.getInstanceHelixDisabledReason(CLUSTER_NAME + "localhost_12918"), "reason_1");

    instancesToDisable = Arrays
        .asList(new String[]{CLUSTER_NAME + "localhost_12918", CLUSTER_NAME + "localhost_12920"});
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(ImmutableMap
            .of(InstancesAccessor.InstancesProperties.instances.name(), instancesToDisable)),
        MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/instances", ImmutableMap
        .of("command", "enable", "instanceDisabledType", "USER_OPERATION", "instanceDisabledReason",
            "reason_1"), entity, Response.Status.OK.getStatusCode());
    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertEquals(clusterConfig.getDisabledInstances().keySet(),
        new HashSet<>(Arrays.asList(CLUSTER_NAME + "localhost_12919")));
    Assert.assertEquals(clusterConfig.getDisabledInstancesWithInfo().keySet(),
        new HashSet<>(Arrays.asList(CLUSTER_NAME + "localhost_12919")));
    Assert.assertEquals(Long.parseLong(
        clusterConfig.getInstanceHelixDisabledTimeStamp(CLUSTER_NAME + "localhost_12919")),
        Long.parseLong(clusterConfig.getDisabledInstances().get(CLUSTER_NAME + "localhost_12919")));
    Assert
        .assertEquals(clusterConfig.getInstanceHelixDisabledType(CLUSTER_NAME + "localhost_12918"),
            "INSTANCE_NOT_DISABLED");
    Assert
        .assertNull(clusterConfig.getInstanceHelixDisabledReason(CLUSTER_NAME + "localhost_12918"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetAllInstances")
  public void testValidateWeightForAllInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Empty out ClusterConfig's weight key setting and InstanceConfig's capacity maps for testing
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord().setListField(
        ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name(), new ArrayList<>());
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    for (String instance : instances) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setInstanceCapacityMap(Collections.emptyMap());
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }

    // Issue a validate call
    String body = new JerseyUriRequestBuilder("clusters/{}/instances?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    // Must have the results saying they are all valid (true) because there's no capacity keys set
    // in ClusterConfig
    node.iterator().forEachRemaining(child -> Assert.assertTrue(child.booleanValue()));

    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    body = new JerseyUriRequestBuilder("clusters/{}/instances?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode()).get(this);
    node = OBJECT_MAPPER.readTree(body);
    // Since instances do not have weight-related configs, the result should return error
    Assert.assertTrue(node.has("error"));

    // Now set weight-related configs in InstanceConfigs
    instances = _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
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

  @Test(dependsOnMethods = "testValidateWeightForAllInstances")
  public void testMultipleReplicasInSameMZ() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Create SemiAuto DB so that we can control assignment
    String testDb = TestHelper.getTestMethodName() + "_resource";
    _gSetupTool.getClusterManagementTool().addResource(STOPPABLE_CLUSTER2, testDb, 3, "MasterSlave",
        IdealState.RebalanceMode.SEMI_AUTO.toString());
    _gSetupTool.getClusterManagementTool().rebalance(STOPPABLE_CLUSTER2, testDb, 3);

    // Manually set ideal state to have the 3 replcias assigned to 3 instances all in the same zone
    List<String> preferenceList = Arrays.asList("instance0", "instance1", "instance2");
    IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(STOPPABLE_CLUSTER2, testDb);
    for (String p : is.getPartitionSet()) {
      is.setPreferenceList(p, preferenceList);
    }
    is.setMinActiveReplicas(2);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(STOPPABLE_CLUSTER2, testDb, is);

    // Wait for assignments to take place
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(STOPPABLE_CLUSTER2).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // Run stoppable check against the 3 instances where SemiAuto DB was assigned
    String content =
        String.format("{\"%s\":\"%s\",\"%s\":[\"%s\",\"%s\",\"%s\"]}",
            InstancesAccessor.InstancesProperties.selection_base.name(),
            InstancesAccessor.InstanceHealthSelectionBase.zone_based.name(),
            InstancesAccessor.InstancesProperties.instances.name(), "instance0", "instance1",
            "instance2");
    Response response =
        new JerseyUriRequestBuilder("clusters/{}/instances?command=stoppable&skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
            STOPPABLE_CLUSTER2).post(this, Entity.entity(content, MediaType.APPLICATION_JSON_TYPE));
    JsonNode jsonNode = OBJECT_MAPPER.readTree(response.readEntity(String.class));

    // Resource has 3 replicas with min_active of 2
    // First instance should be stoppable as min_active still satisfied
    Set<String> stoppableSet = getStringSet(jsonNode,
        InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    Assert.assertTrue(Collections.singleton("instance0").equals(stoppableSet));

    // Next 2 instances should fail stoppable due to MIN_ACTIVE_REPLICA_CHECK_FAILED
    JsonNode nonStoppableInstances = jsonNode.get(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Assert.assertFalse(getStringSet(nonStoppableInstances, "instance0")
        .contains("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertTrue(getStringSet(nonStoppableInstances, "instance1")
        .contains("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    Assert.assertTrue(getStringSet(nonStoppableInstances, "instance2")
        .contains("HELIX:MIN_ACTIVE_REPLICA_CHECK_FAILED"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  private Set<String> getStringSet(JsonNode jsonNode, String key) {
    Set<String> result = new HashSet<>();
    jsonNode.withArray(key).forEach(s -> result.add(s.textValue()));
    return result;
  }
}
