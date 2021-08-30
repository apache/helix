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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.resources.helix.ResourceAssignmentOptimizerAccessor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestResourceAssignmentOptimizerAccessor extends AbstractTestClass {

  String cluster = "TestCluster_3";
  String instance1 = cluster + "dummyInstance_localhost_12930";
  String urlBase = "clusters/TestCluster_3/partitionAssignment/";
  String toDeactivatedInstance, toEnabledInstance;
  HelixDataAccessor helixDataAccessor;
  List<String> resources;
  List<String> liveInstances;

  @BeforeClass
  public void beforeClass() {
    helixDataAccessor = new ZKHelixDataAccessor(cluster, _baseAccessor);
    _gSetupTool.addInstanceToCluster(cluster, instance1);
    resources = _gSetupTool.getClusterManagementTool().getResourcesInCluster(cluster);
    liveInstances =  helixDataAccessor.getChildNames(helixDataAccessor.keyBuilder().liveInstances());
    Assert.assertFalse(resources.isEmpty() || liveInstances.isEmpty());

    // set up instances, we need too deactivate one instance
    toDeactivatedInstance = liveInstances.get(0);
    toEnabledInstance = liveInstances.get(2);
    InstanceConfig config = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(cluster, toEnabledInstance);
    config.setInstanceEnabled(false);
    _gSetupTool.getClusterManagementTool()
        .setInstanceConfig(cluster, toEnabledInstance, config);

    // set all resource to FULL_AUTO
    for (String resource : resources) {
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setDelayRebalanceEnabled(true);
      idealState.setRebalanceDelay(360000);
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, resource, idealState);
    }

  }

  @AfterClass
  public void afterClass() {
    for (String resource : resources) {
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, resource, idealState);
    }
    InstanceConfig config = _gSetupTool.getClusterManagementTool()
        .getInstanceConfig(cluster, toEnabledInstance);
    config.setInstanceEnabled(true);
    _gSetupTool.getClusterManagementTool().setInstanceConfig(cluster, toEnabledInstance, config);
    _gSetupTool.getClusterManagementTool()
        .enableMaintenanceMode(cluster, false, TestHelper.getTestMethodName());
  }

  @Test
  public void testComputePartitionAssignment() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Test AddInstances, RemoveInstances and SwapInstances
    String payload = "{\"InstanceChange\" : {  \"ActivateInstances\" : [\"" + toEnabledInstance + "\"],"
        + "\"DeactivateInstances\" : [ \"" + toDeactivatedInstance + "\"] }}  ";
    Response response = post(urlBase, null, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true);
    Map<String, Map<String, Map<String, String>>> resourceAssignments = OBJECT_MAPPER
        .readValue(response.readEntity(String.class),
            new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
            });
    Set<String> hostSet = new HashSet<>();
    resourceAssignments.forEach((k, v) -> v.forEach((kk, vv) -> hostSet.addAll(vv.keySet())));
    resourceAssignments.forEach((k, v) -> v.forEach((kk, vv) -> Assert.assertEquals(vv.size(), 2)));
    Assert.assertTrue(hostSet.contains(toEnabledInstance));
    Assert.assertFalse(hostSet.contains(toDeactivatedInstance));
    // Validate header
    MultivaluedMap<String, Object> headers = response.getHeaders();
    Assert.assertTrue(headers.containsKey(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY));
    Assert.assertFalse(
        headers.get(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY).isEmpty());
    Assert.assertEquals(headers.get(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY).get(0),
        "{instanceFilter=[], resourceFilter=[], returnFormat=IdealStateFormat}");

    // Test partitionAssignment InstanceFilter
    String payload2 = "{\"Options\" : { \"InstanceFilter\" : [\"" + liveInstances.get(0) + "\" , \""
        + liveInstances.get(1) + "\"] }}  ";
    Response response2 = post(urlBase, null, Entity.entity(payload2, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true);
    String body2 = response2.readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments2 = OBJECT_MAPPER
        .readValue(body2, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    Set<String> hostSet2 = new HashSet<>();
    resourceAssignments2.forEach((k, v) -> v.forEach((kk, vv) -> hostSet2.addAll(vv.keySet())));
    Assert.assertEquals(hostSet2.size(), 2);
    Assert.assertTrue(hostSet2.contains(liveInstances.get(0)));
    Assert.assertTrue(hostSet2.contains(liveInstances.get(1)));
    // Validate header
    MultivaluedMap<String, Object> headers2 = response2.getHeaders();
    Assert
        .assertTrue(headers2.containsKey(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY));
    List partitionAssignmentMetadata2 =
        headers2.get(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY);
    Assert.assertFalse(
        headers2.get(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY).isEmpty());
    Assert.assertTrue(
        partitionAssignmentMetadata2.get(0).equals(
            "{instanceFilter=[" + liveInstances.get(0) + ", " + liveInstances.get(1)
            + "], resourceFilter=[], returnFormat=IdealStateFormat}") ||
        partitionAssignmentMetadata2.get(0).equals(
            "{instanceFilter=[" + liveInstances.get(1) + ", " + liveInstances.get(0)
                + "], resourceFilter=[], returnFormat=IdealStateFormat}"),
        partitionAssignmentMetadata2.get(0).toString());

    // Test partitionAssignment ResourceFilter
    String payload3 =
        "{\"Options\" : { \"ResourceFilter\" : [\"" + resources.get(0) + "\" , \"" + resources
            .get(1) + "\"] }}  ";
    Response response3 =
        post(urlBase, null, Entity.entity(payload3, MediaType.APPLICATION_JSON_TYPE),
            Response.Status.OK.getStatusCode(), true);
    String body3 = response3.readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments3 = OBJECT_MAPPER
        .readValue(body3, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    Assert.assertEquals(resourceAssignments3.size(), 2);
    Assert.assertTrue(resourceAssignments3.containsKey(resources.get(0)));
    Assert.assertTrue(resourceAssignments3.containsKey(resources.get(1)));
    // Validate header
    MultivaluedMap<String, Object> headers3 = response3.getHeaders();
    Assert
        .assertTrue(headers3.containsKey(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY));
    List partitionAssignmentMetadata3 =
        headers3.get(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY);
    Assert.assertFalse(
        headers3.get(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY).isEmpty());
    Assert.assertTrue(
        partitionAssignmentMetadata3.get(0).equals(
            "{instanceFilter=[], resourceFilter=[" + resources.get(0) + ", " + resources.get(1)
                + "], returnFormat=IdealStateFormat}") ||
        partitionAssignmentMetadata3.get(0).equals(
                "{instanceFilter=[], resourceFilter=[" + resources.get(1) + ", " + resources.get(0)
                    + "], returnFormat=IdealStateFormat}"),
        partitionAssignmentMetadata3.get(0).toString());

    // Test Option CurrentState format with AddInstances, RemoveInstances and SwapInstances
    String payload4 = "{\"InstanceChange\" : { \"ActivateInstances\" : [\"" + toEnabledInstance
        + "\"], \"DeactivateInstances\" : [ \"" + toDeactivatedInstance + "\"] "
        + "}, \"Options\" : { \"ReturnFormat\" : \"CurrentStateFormat\" , \"ResourceFilter\" : [\""
        + resources.get(0) + "\" , \"" + resources.get(1) + "\"]} } ";
    Response response4 =
        post(urlBase, null, Entity.entity(payload4, MediaType.APPLICATION_JSON_TYPE),
            Response.Status.OK.getStatusCode(), true);
    String body4 = response4.readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments4 = OBJECT_MAPPER
        .readValue(body4, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    // Validate target resources exist
    Set<String> resource4 = new HashSet<>();
    resourceAssignments4.forEach((k, v) -> v.forEach((kk, vv) -> resource4.add(kk)));
    Assert.assertTrue(resource4.contains(resources.get(0)));
    Assert.assertTrue(resource4.contains(resources.get(1)));
    // Validate header
    MultivaluedMap<String, Object> headers4 = response4.getHeaders();
    Assert
        .assertTrue(headers4.containsKey(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY));
    List partitionAssignmentMetadata4 =
        headers4.get(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY);
    Assert.assertFalse(
        headers4.get(ResourceAssignmentOptimizerAccessor.RESPONSE_HEADER_KEY).isEmpty());
    Assert.assertTrue(
        partitionAssignmentMetadata4.get(0).equals(
            "{instanceFilter=[], resourceFilter=[" + resources.get(0) + ", " + resources.get(1)
                + "], returnFormat=CurrentStateFormat}") ||
        partitionAssignmentMetadata4.get(0).equals(
                "{instanceFilter=[], resourceFilter=[" + resources.get(1) + ", " + resources.get(0)
                    + "], returnFormat=CurrentStateFormat}"),
        partitionAssignmentMetadata4.get(0).toString());

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testComputePartitionAssignment")
  public void testComputePartitionAssignmentWaged() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Use Waged for following tests
    for (String resource : resources) {
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
      idealState
          .setRebalancerClassName("org.apache.helix.controller.rebalancer.waged.WagedRebalancer");
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, resource, idealState);
    }

    // Test AddInstances, RemoveInstances and SwapInstances
    String payload = "{\"InstanceChange\" : {  \"ActivateInstances\" : [\"" + toEnabledInstance
        + "\"], \"DeactivateInstances\" : [ \"" + toDeactivatedInstance + "\"] }}  ";
    String body = post(urlBase, null, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true).readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments = OBJECT_MAPPER
        .readValue(body, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    Set<String> hostSet = new HashSet<>();
    resourceAssignments.forEach((k, v) -> v.forEach((kk, vv) -> hostSet.addAll(vv.keySet())));
    // Assert every partition has 2 replicas. Indicating we ignore the delayed rebalance when
    // recomputing partition assignment.
    resourceAssignments.forEach((k, v) -> v.forEach((kk, vv) -> Assert.assertEquals(vv.size(), 2)));
    Assert.assertTrue(hostSet.contains(toEnabledInstance));
    Assert.assertFalse(hostSet.contains(toDeactivatedInstance));

    // Test partitionAssignment host filter
    String payload2 = "{\"Options\" : { \"InstanceFilter\" : [\"" + liveInstances.get(0) + "\" , \""
        + liveInstances.get(1) + "\"] }}  ";
    String body2 = post(urlBase, null, Entity.entity(payload2, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true).readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments2 = OBJECT_MAPPER
        .readValue(body2, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    Set<String> hostSet2 = new HashSet<>();
    resourceAssignments2.forEach((k, v) -> v.forEach((kk, vv) -> hostSet2.addAll(vv.keySet())));
    Assert.assertEquals(hostSet2.size(), 2);
    Assert.assertTrue(hostSet2.contains(liveInstances.get(0)));
    Assert.assertTrue(hostSet2.contains(liveInstances.get(1)));

    String payload3 =
        "{\"Options\" : { \"ResourceFilter\" : [\"" + resources.get(0) + "\" , \"" + resources
            .get(1) + "\"] }}  ";
    String body3 = post(urlBase, null, Entity.entity(payload3, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true).readEntity(String.class);
    Map<String, Map<String, Map<String, String>>> resourceAssignments3 = OBJECT_MAPPER
        .readValue(body3, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    Assert.assertEquals(resourceAssignments3.size(), 2);
    Assert.assertTrue(resourceAssignments3.containsKey(resources.get(0)));
    Assert.assertTrue(resourceAssignments3.containsKey(resources.get(1)));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testComputePartitionAssignmentWaged")
  public void testComputePartitionAssignmentNegativeInput() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // Test negative input
    String payload4 = "{\"InstanceChange\" : { \"ActivateInstances\" : [\" nonExistInstanceName \"] }} ";
    post(urlBase, null, Entity.entity(payload4, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode(), true);

    String payload5 =
        "{\"InstanceChange\" : {  { \"ActivateInstances\" : [\"" + toDeactivatedInstance
            + "\"], \"DeactivateInstances\" : [\"" + toDeactivatedInstance + "\"] }} ";
    post(urlBase, null, Entity.entity(payload5, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode(), true);

    // Currently we do not support maintenance mode
    _gSetupTool.getClusterManagementTool()
        .enableMaintenanceMode(cluster, true, TestHelper.getTestMethodName());
    String payload6 = "{}";
    post(urlBase, null, Entity.entity(payload6, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode(), true);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}