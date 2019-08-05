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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.rest.server.resources.helix.ResourceAccessor;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestResourceAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";
  private final static String RESOURCE_NAME = CLUSTER_NAME + "_db_0";
  private final static String ANY_INSTANCE = "ANY_LIVEINSTANCE";

  @Test
  public void testGetResources() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/resources", null,
        Response.Status.OK.getStatusCode(), true);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String idealStates =
        node.get(ResourceAccessor.ResourceProperties.idealStates.name()).toString();
    Assert.assertNotNull(idealStates);

    Set<String> resources = OBJECT_MAPPER.readValue(idealStates,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(resources, _resourcesMap.get("TestCluster_0"), "Resources from response: "
        + resources + " vs clusters actually: " + _resourcesMap.get("TestCluster_0"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetResources")
  public void testGetResource() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = get("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME, null,
        Response.Status.OK.getStatusCode(), true);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String idealStateStr =
        node.get(ResourceAccessor.ResourceProperties.idealState.name()).toString();
    IdealState idealState = new IdealState(toZNRecord(idealStateStr));
    IdealState originIdealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
    Assert.assertEquals(idealState, originIdealState);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetResource")
  public void testAddResources() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String newResourceName = "newResource";
    IdealState idealState = new IdealState(newResourceName);
    idealState.getRecord().getSimpleFields().putAll(_gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME).getRecord().getSimpleFields());

    // Add resource by IdealState
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(idealState.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    put("clusters/" + CLUSTER_NAME + "/resources/" + newResourceName, null, entity,
        Response.Status.OK.getStatusCode());

    Assert.assertEquals(idealState, _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, newResourceName));

    // Add resource by query param
    entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);

    put("clusters/" + CLUSTER_NAME + "/resources/" + newResourceName + "0", ImmutableMap
        .of("numPartitions", "4", "stateModelRef", "OnlineOffline", "rebalancerMode", "FULL_AUTO"),
        entity, Response.Status.OK.getStatusCode());

    IdealState queryIdealState = new FullAutoModeISBuilder(newResourceName + 0).setNumPartitions(4)
        .setStateModel("OnlineOffline").setRebalancerMode(IdealState.RebalanceMode.FULL_AUTO)
        .setRebalanceStrategy("DEFAULT").build();
    Assert.assertEquals(queryIdealState, _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, newResourceName + "0"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddResources")
  public void testResourceConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/configs", null,
        Response.Status.OK.getStatusCode(), true);
    ResourceConfig resourceConfig = new ResourceConfig(toZNRecord(body));
    Assert.assertEquals(resourceConfig,
        _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testResourceConfig")
  public void testIdealState() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/idealState",
        null, Response.Status.OK.getStatusCode(), true);
    IdealState idealState = new IdealState(toZNRecord(body));
    Assert.assertEquals(idealState,
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testIdealState")
  public void testExternalView() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String body = get("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/externalView",
        null, Response.Status.OK.getStatusCode(), true);
    ExternalView externalView = new ExternalView(toZNRecord(body));
    Assert.assertEquals(externalView, _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, RESOURCE_NAME));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testExternalView")
  public void testPartitionHealth() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String clusterName = "TestCluster_1";
    String resourceName = clusterName + "_db_0";

    // Use mock numbers for testing
    Map<String, String> idealStateParams = new HashMap<>();
    idealStateParams.put("MinActiveReplicas", "2");
    idealStateParams.put("StateModelDefRef", "MasterSlave");
    idealStateParams.put("MaxPartitionsPerInstance", "3");
    idealStateParams.put("Replicas", "3");
    idealStateParams.put("NumPartitions", "3");

    // Create a mock state mapping for testing
    Map<String, List<String>> partitionReplicaStates = new LinkedHashMap<>();
    String[] p0 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p1 = {
        "MASTER", "SLAVE", "ERROR"
    };
    String[] p2 = {
        "ERROR", "SLAVE", "SLAVE"
    };
    partitionReplicaStates.put("p0", Arrays.asList(p0));
    partitionReplicaStates.put("p1", Arrays.asList(p1));
    partitionReplicaStates.put("p2", Arrays.asList(p2));

    createDummyMapping(clusterName, resourceName, idealStateParams, partitionReplicaStates);

    // Get the result of getPartitionHealth
    String body = get("clusters/" + clusterName + "/resources/" + resourceName + "/health", null,
        Response.Status.OK.getStatusCode(), true);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    Map<String, String> healthStatus =
        OBJECT_MAPPER.readValue(node, new TypeReference<Map<String, String>>() {
        });

    Assert.assertEquals(healthStatus.get("p0"), "HEALTHY");
    Assert.assertEquals(healthStatus.get("p1"), "PARTIAL_HEALTHY");
    Assert.assertEquals(healthStatus.get("p2"), "UNHEALTHY");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testPartitionHealth")
  public void testResourceHealth() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    String clusterName = "TestCluster_1";
    Map<String, String> idealStateParams = new HashMap<>();
    idealStateParams.put("MinActiveReplicas", "2");
    idealStateParams.put("StateModelDefRef", "MasterSlave");
    idealStateParams.put("MaxPartitionsPerInstance", "3");
    idealStateParams.put("Replicas", "3");
    idealStateParams.put("NumPartitions", "3");

    // Create a healthy resource
    String resourceNameHealthy = clusterName + "_db_0";
    Map<String, List<String>> partitionReplicaStates = new LinkedHashMap<>();
    String[] p0 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p1 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p2 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    partitionReplicaStates.put("p0", Arrays.asList(p0));
    partitionReplicaStates.put("p1", Arrays.asList(p1));
    partitionReplicaStates.put("p2", Arrays.asList(p2));

    createDummyMapping(clusterName, resourceNameHealthy, idealStateParams, partitionReplicaStates);

    // Create a partially healthy resource
    String resourceNamePartiallyHealthy = clusterName + "_db_1";
    Map<String, List<String>> partitionReplicaStates_1 = new LinkedHashMap<>();
    String[] p0_1 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p1_1 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p2_1 = {
        "MASTER", "SLAVE", "ERROR"
    };
    partitionReplicaStates_1.put("p0", Arrays.asList(p0_1));
    partitionReplicaStates_1.put("p1", Arrays.asList(p1_1));
    partitionReplicaStates_1.put("p2", Arrays.asList(p2_1));

    createDummyMapping(clusterName, resourceNamePartiallyHealthy, idealStateParams,
        partitionReplicaStates_1);

    // Create a partially healthy resource
    String resourceNameUnhealthy = clusterName + "_db_2";
    Map<String, List<String>> partitionReplicaStates_2 = new LinkedHashMap<>();
    String[] p0_2 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p1_2 = {
        "MASTER", "SLAVE", "SLAVE"
    };
    String[] p2_2 = {
        "ERROR", "SLAVE", "ERROR"
    };
    partitionReplicaStates_2.put("p0", Arrays.asList(p0_2));
    partitionReplicaStates_2.put("p1", Arrays.asList(p1_2));
    partitionReplicaStates_2.put("p2", Arrays.asList(p2_2));

    createDummyMapping(clusterName, resourceNameUnhealthy, idealStateParams,
        partitionReplicaStates_2);

    // Get the result of getResourceHealth
    String body = get("clusters/" + clusterName + "/resources/health", null,
        Response.Status.OK.getStatusCode(), true);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    Map<String, String> healthStatus =
        OBJECT_MAPPER.readValue(node, new TypeReference<Map<String, String>>() {
        });

    Assert.assertEquals(healthStatus.get(resourceNameHealthy), "HEALTHY");
    Assert.assertEquals(healthStatus.get(resourceNamePartiallyHealthy), "PARTIAL_HEALTHY");
    Assert.assertEquals(healthStatus.get(resourceNameUnhealthy), "UNHEALTHY");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "update" command of updateResourceConfig.
   * @throws Exception
   */
  @Test(dependsOnMethods = "testResourceHealth")
  public void updateResourceConfig() throws Exception {
    // Get ResourceConfig
    ResourceConfig resourceConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME);
    ZNRecord record = resourceConfig.getRecord();

    // Generate a record containing three keys (k0, k1, k2) for all fields
    String value = "RESOURCE_TEST";
    for (int i = 0; i < 3; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, value);
      record.getMapFields().put(key, ImmutableMap.of(key, value));
      record.getListFields().put(key, Arrays.asList(key, value));
    }

    // 1. Add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/configs",
        Collections.singletonMap("command", "update"), entity, Response.Status.OK.getStatusCode());

    // Check that the fields have been added
    ResourceConfig updatedConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME);
    Assert.assertEquals(record.getSimpleFields(), updatedConfig.getRecord().getSimpleFields());
    Assert.assertEquals(record.getListFields(), updatedConfig.getRecord().getListFields());
    Assert.assertEquals(record.getMapFields(), updatedConfig.getRecord().getMapFields());

    String newValue = "newValue";
    // 2. Modify the record and update
    for (int i = 0; i < 3; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, newValue);
      record.getMapFields().put(key, ImmutableMap.of(key, newValue));
      record.getListFields().put(key, Arrays.asList(key, newValue));
    }

    entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/configs",
        Collections.singletonMap("command", "update"), entity, Response.Status.OK.getStatusCode());

    updatedConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME);
    // Check that the fields have been modified
    Assert.assertEquals(record.getSimpleFields(), updatedConfig.getRecord().getSimpleFields());
    Assert.assertEquals(record.getListFields(), updatedConfig.getRecord().getListFields());
    Assert.assertEquals(record.getMapFields(), updatedConfig.getRecord().getMapFields());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "delete" command of updateResourceConfig.
   * @throws Exception
   */
  @Test(dependsOnMethods = "updateResourceConfig")
  public void deleteFromResourceConfig() throws Exception {
    ZNRecord record = new ZNRecord(RESOURCE_NAME);

    // Generate a record containing three keys (k1, k2, k3) for all fields for deletion
    String value = "value";
    for (int i = 1; i < 4; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, value);
      record.getMapFields().put(key, ImmutableMap.of(key, value));
      record.getListFields().put(key, Arrays.asList(key, value));
    }

    // First, add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/configs",
        Collections.singletonMap("command", "delete"), entity, Response.Status.OK.getStatusCode());

    ResourceConfig configAfterDelete =
        _configAccessor.getResourceConfig(CLUSTER_NAME, RESOURCE_NAME);

    // Check that the keys k1 and k2 have been deleted, and k0 remains
    for (int i = 0; i < 4; i++) {
      String key = "k" + i;
      if (i == 0) {
        Assert.assertTrue(configAfterDelete.getRecord().getSimpleFields().containsKey(key));
        Assert.assertTrue(configAfterDelete.getRecord().getListFields().containsKey(key));
        Assert.assertTrue(configAfterDelete.getRecord().getMapFields().containsKey(key));
        continue;
      }
      Assert.assertFalse(configAfterDelete.getRecord().getSimpleFields().containsKey(key));
      Assert.assertFalse(configAfterDelete.getRecord().getListFields().containsKey(key));
      Assert.assertFalse(configAfterDelete.getRecord().getMapFields().containsKey(key));
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "update" command of updateResourceIdealState.
   * @throws Exception
   */
  @Test(dependsOnMethods = "deleteFromResourceConfig")
  public void updateResourceIdealState() throws Exception {
    // Get IdealState ZNode
    String zkPath = PropertyPathBuilder.idealState(CLUSTER_NAME, RESOURCE_NAME);
    ZNRecord record = _baseAccessor.get(zkPath, null, AccessOption.PERSISTENT);

    // 1. Add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/idealState",
        Collections.singletonMap("command", "update"), entity, Response.Status.OK.getStatusCode());

    // Check that the fields have been added
    ZNRecord newRecord = _baseAccessor.get(zkPath, null, AccessOption.PERSISTENT);
    Assert.assertEquals(record.getSimpleFields(), newRecord.getSimpleFields());
    Assert.assertEquals(record.getListFields(), newRecord.getListFields());
    Assert.assertEquals(record.getMapFields(), newRecord.getMapFields());

    String newValue = "newValue";
    // 2. Modify the record and update
    for (int i = 0; i < 3; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, newValue);
      record.getMapFields().put(key, ImmutableMap.of(key, newValue));
      record.getListFields().put(key, Arrays.asList(key, newValue));
    }

    entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/idealState",
        Collections.singletonMap("command", "update"), entity, Response.Status.OK.getStatusCode());

    // Check that the fields have been modified
    newRecord = _baseAccessor.get(zkPath, null, AccessOption.PERSISTENT);
    Assert.assertEquals(record.getSimpleFields(), newRecord.getSimpleFields());
    Assert.assertEquals(record.getListFields(), newRecord.getListFields());
    Assert.assertEquals(record.getMapFields(), newRecord.getMapFields());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "delete" command of updateResourceIdealState.
   * @throws Exception
   */
  @Test(dependsOnMethods = "updateResourceIdealState")
  public void deleteFromResourceIdealState() throws Exception {
    String zkPath = PropertyPathBuilder.idealState(CLUSTER_NAME, RESOURCE_NAME);
    ZNRecord record = new ZNRecord(RESOURCE_NAME);

    // Generate a record containing three keys (k1, k2, k3) for all fields for deletion
    String value = "value";
    for (int i = 1; i < 4; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, value);
      record.getMapFields().put(key, ImmutableMap.of(key, value));
      record.getListFields().put(key, Arrays.asList(key, value));
    }

    // First, add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    post("clusters/" + CLUSTER_NAME + "/resources/" + RESOURCE_NAME + "/idealState",
        Collections.singletonMap("command", "delete"), entity, Response.Status.OK.getStatusCode());

    ZNRecord recordAfterDelete = _baseAccessor.get(zkPath, null, AccessOption.PERSISTENT);

    // Check that the keys k1 and k2 have been deleted, and k0 remains
    for (int i = 0; i < 4; i++) {
      String key = "k" + i;
      if (i == 0) {
        Assert.assertTrue(recordAfterDelete.getSimpleFields().containsKey(key));
        Assert.assertTrue(recordAfterDelete.getListFields().containsKey(key));
        Assert.assertTrue(recordAfterDelete.getMapFields().containsKey(key));
        continue;
      }
      Assert.assertFalse(recordAfterDelete.getSimpleFields().containsKey(key));
      Assert.assertFalse(recordAfterDelete.getListFields().containsKey(key));
      Assert.assertFalse(recordAfterDelete.getMapFields().containsKey(key));
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Creates a setup where the health API can be tested.
   * @param clusterName
   * @param resourceName
   * @param idealStateParams
   * @param partitionReplicaStates maps partitionName to its replicas' states
   * @throws Exception
   */
  private void createDummyMapping(String clusterName, String resourceName,
      Map<String, String> idealStateParams, Map<String, List<String>> partitionReplicaStates)
      throws Exception {
    IdealState idealState = new IdealState(resourceName);
    idealState.setMinActiveReplicas(Integer.parseInt(idealStateParams.get("MinActiveReplicas"))); // 2
    idealState.setStateModelDefRef(idealStateParams.get("StateModelDefRef")); // MasterSlave
    idealState.setMaxPartitionsPerInstance(
        Integer.parseInt(idealStateParams.get("MaxPartitionsPerInstance"))); // 3
    idealState.setReplicas(idealStateParams.get("Replicas")); // 3
    idealState.setNumPartitions(Integer.parseInt(idealStateParams.get("NumPartitions"))); // 3
    idealState.enable(false);

    Map<String, List<String>> partitionNames = new LinkedHashMap<>();
    List<String> dummyPrefList = new ArrayList<>();

    for (int i = 0; i < Integer.parseInt(idealStateParams.get("MaxPartitionsPerInstance")); i++) {
      dummyPrefList.add(ANY_INSTANCE);
      partitionNames.put("p" + i, dummyPrefList);
    }
    idealState.getRecord().getListFields().putAll(partitionNames);

    if (!_gSetupTool.getClusterManagementTool().getClusters().contains(clusterName)) {
      _gSetupTool.getClusterManagementTool().addCluster(clusterName);
    }
    _gSetupTool.getClusterManagementTool().setResourceIdealState(clusterName, resourceName,
        idealState);

    // Set ExternalView's replica states for a given parameter map
    ExternalView externalView = new ExternalView(resourceName);

    Map<String, Map<String, String>> mappingCurrent = new LinkedHashMap<>();

    List<String> partitionReplicaStatesList = new ArrayList<>(partitionReplicaStates.keySet());
    for (int k = 0; k < partitionReplicaStatesList.size(); k++) {
      Map<String, String> replicaStatesForPartition = new LinkedHashMap<>();
      List<String> replicaStateList = partitionReplicaStates.get(partitionReplicaStatesList.get(k));
      for (int i = 0; i < replicaStateList.size(); i++) {
        replicaStatesForPartition.put("r" + i, replicaStateList.get(i));
      }
      mappingCurrent.put("p" + k, replicaStatesForPartition);
    }

    externalView.getRecord().getMapFields().putAll(mappingCurrent);

    HelixManager helixManager = HelixManagerFactory.getZKHelixManager(clusterName, "p1",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    helixManager.connect();
    HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
    helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().externalView(resourceName),
        externalView);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}
