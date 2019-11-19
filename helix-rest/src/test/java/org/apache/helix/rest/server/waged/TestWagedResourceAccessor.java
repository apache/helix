package org.apache.helix.rest.server.waged;

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
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.rest.server.AbstractTestClass;
import org.apache.helix.rest.server.resources.helix.ResourceAccessor;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestWagedResourceAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";
  private final static String RESOURCE_NAME = CLUSTER_NAME + "_db_0";

  @BeforeClass
  public void beforeClass() {
    // Set up WAGED rebalancer specific configs
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  /**
   * Test that addResourceWithWeight works as expected via REST.
   * @throws IOException
   */
  @Test
  public void testAddResourceWithWeight()
      throws IOException {
    // Test case 1: Add a valid resource with valid weights
    // Create a resource with IdealState and ResourceConfig
    String wagedResourceName = "newWagedResource";

    // Create an IdealState on full-auto with 1 partition
    IdealState idealState = new IdealState(wagedResourceName);
    idealState.getRecord().getSimpleFields().putAll(
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME)
            .getRecord().getSimpleFields());
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    idealState.setNumPartitions(1); // 1 partition for convenience of testing

    // Create a ResourceConfig with FOO and BAR at 100 respectively
    ResourceConfig resourceConfig = new ResourceConfig(wagedResourceName);
    Map<String, Map<String, Integer>> partitionCapacityMap = new HashMap<>();
    Map<String, Integer> partitionCapacity = ImmutableMap.of("FOO", 100, "BAR", 100);
    partitionCapacityMap.put(wagedResourceName + "_0", partitionCapacity);
    // Also add a default key
    partitionCapacityMap.put(ResourceConfig.DEFAULT_PARTITION_KEY, partitionCapacity);
    resourceConfig.setPartitionCapacityMap(partitionCapacityMap);

    // Put both IdealState and ResourceConfig into a map as required
    Map<String, ZNRecord> inputMap = ImmutableMap
        .of(ResourceAccessor.ResourceProperties.idealState.name(), idealState.getRecord(),
            ResourceAccessor.ResourceProperties.resourceConfig.name(), resourceConfig.getRecord());

    // Create an entity using the inputMap
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(inputMap), MediaType.APPLICATION_JSON_TYPE);

    // Make a HTTP call to the REST endpoint
    put("clusters/" + CLUSTER_NAME + "/waged/resources/" + wagedResourceName, null, entity,
        Response.Status.OK.getStatusCode());

    // Test case 2: Add a resource with invalid weights
    String invalidResourceName = "invalidWagedResource";
    ResourceConfig invalidWeightResourceConfig = new ResourceConfig(invalidResourceName);
    IdealState invalidWeightIdealState = new IdealState(invalidResourceName);

    Map<String, ZNRecord> invalidInputMap = ImmutableMap
        .of(ResourceAccessor.ResourceProperties.idealState.name(),
            invalidWeightIdealState.getRecord(),
            ResourceAccessor.ResourceProperties.resourceConfig.name(),
            invalidWeightResourceConfig.getRecord());

    // Create an entity using invalidInputMap
    entity = Entity
        .entity(OBJECT_MAPPER.writeValueAsString(invalidInputMap), MediaType.APPLICATION_JSON_TYPE);

    // Make a HTTP call to the REST endpoint
    put("clusters/" + CLUSTER_NAME + "/waged/resources/" + invalidResourceName, null, entity,
        Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test(dependsOnMethods = "testAddResourceWithWeight")
  public void testValidateResource()
      throws IOException {
    // Validate the resource added in testAddResourceWithWeight()
    String resourceToValidate = "newWagedResource";
    // This should fail because none of the instances have weight configured
    get("clusters/" + CLUSTER_NAME + "/waged/resources/" + resourceToValidate + "/validate", null,
        Response.Status.BAD_REQUEST.getStatusCode(), true);

    // Add weight configurations to all instance configs
    Map<String, Integer> instanceCapacityMap = ImmutableMap.of("FOO", 1000, "BAR", 1000);
    for (String instance : _instancesMap.get(CLUSTER_NAME)) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setInstanceCapacityMap(instanceCapacityMap);
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }

    // Now try validating again - it should go through and return a 200
    String body =
        get("clusters/" + CLUSTER_NAME + "/waged/resources/" + resourceToValidate + "/validate",
            null, Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertEquals(node.get(resourceToValidate).toString(), "true");
  }
}
