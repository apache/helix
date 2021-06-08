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
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestResourceAssignmentOptimizerAccessor extends AbstractTestClass {

  String cluster = "TestCluster_3";
  String instance1 = cluster + "dummyInstance_localhost_12930";
  String swapNewInstance = "swapNewInstance";
  String urlBase = "clusters/TestCluster_3/partitionAssignment/";
  String swapOldInstance, toRemoveInstance;
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

    toRemoveInstance = liveInstances.get(0);
    swapOldInstance = liveInstances.get(1);
  }

  @AfterClass
  public void afterClass() {
    for (String resource : resources) {
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, resource, idealState);
    }
  }

  @Test
  public void testComputePartitionAssignment() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    // set all resource to FULL_AUTO except one
    for (int i = 0; i < resources.size() - 1; ++i) {
      String resource = resources.get(i);
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, resource);
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, resource, idealState);
    }

    // Test AddInstances, RemoveInstances and SwapInstances
    String payload = "{\"InstanceChange\" : { \"AddInstances\" : [\"" + instance1
        + "\"], \"RemoveInstances\" : [ \"" + toRemoveInstance + "\"], \"SwapInstances\" : {\""
        + swapOldInstance + "\" : \"" + swapNewInstance + "\"} }}  ";
    String body = post(urlBase, null, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true);
    Map<String, Map<String, Map<String, String>>> resourceAssignments = OBJECT_MAPPER
        .readValue(body, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    Set<String> hostSet = new HashSet<>();
    resourceAssignments.forEach((k, v) -> v.forEach((kk, vv) -> hostSet.addAll(vv.keySet())));
    Assert.assertTrue(hostSet.contains(instance1));
    Assert.assertTrue(hostSet.contains(swapNewInstance));
    Assert.assertFalse(hostSet.contains(liveInstances.get(0)));
    Assert.assertFalse(hostSet.contains(liveInstances.get(1)));

    // Test partitionAssignment host filter
    String payload2 = "{\"Options\" : { \"InstanceFilter\" : [\"" + liveInstances.get(0) + "\" , \""
        + liveInstances.get(1) + "\"] }}  ";
    String body2 = post(urlBase, null, Entity.entity(payload2, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true);
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
        Response.Status.OK.getStatusCode(), true);
    Map<String, Map<String, Map<String, String>>> resourceAssignments3 = OBJECT_MAPPER
        .readValue(body3, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    Assert.assertEquals(resourceAssignments3.size(), 2);
    Assert.assertTrue(resourceAssignments3.containsKey(resources.get(0)));
    Assert.assertTrue(resourceAssignments3.containsKey(resources.get(1)));

    // Test Option CurrentState format
    // Test AddInstances, RemoveInstances and SwapInstances
    String payload4 = "{\"InstanceChange\" : { \"AddInstances\" : [\"" + instance1
        + "\"], \"RemoveInstances\" : [ \"" + toRemoveInstance + "\"], \"SwapInstances\" : {\""
        + swapOldInstance + "\" : \"" + swapNewInstance
        + "\"} }, \"Options\" : { \"ReturnFormat\" : \"CurrentStateFormat\" , \"ResourceFilter\" : [\""
        + resources.get(0) + "\" , \"" + resources.get(1) + "\"]} } ";
    String body4 = post(urlBase, null, Entity.entity(payload4, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true);
    Map<String, Map<String, Map<String, String>>> resourceAssignments4 = OBJECT_MAPPER
        .readValue(body4, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    // Validate outer map key is instance
    Set<String> resource4 = new HashSet<>();
    resourceAssignments4.forEach((k, v)  -> v.forEach((kk, vv) -> resource4.add(kk)));
    Assert.assertTrue(resource4.contains(resources.get(0)));
    Assert.assertTrue(resource4.contains(resources.get(1)));

    // First inner map key is resource
    Assert.assertTrue(resourceAssignments4.containsKey(instance1));
    Assert.assertTrue(resourceAssignments4.containsKey(swapNewInstance));
    Assert.assertFalse(resourceAssignments4.containsKey(liveInstances.get(0)));
    Assert.assertFalse(resourceAssignments4.containsKey(liveInstances.get(1)));


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
    String payload = "{\"InstanceChange\" : { \"AddInstances\" : [\"" + instance1
        + "\"], \"RemoveInstances\" : [ \"" + toRemoveInstance + "\"], \"SwapInstances\" : {\""
        + swapOldInstance + "\" : \"" + swapNewInstance + "\"} }}  ";
    String body = post(urlBase, null, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true);
    Map<String, Map<String, Map<String, String>>> resourceAssignments = OBJECT_MAPPER
        .readValue(body, new TypeReference<HashMap<String, Map<String, Map<String, String>>>>() {
        });
    Set<String> hostSet = new HashSet<>();
    resourceAssignments.forEach((k, v) -> v.forEach((kk, vv) -> hostSet.addAll(vv.keySet())));
    Assert.assertTrue(hostSet.contains(instance1));
    Assert.assertTrue(hostSet.contains(swapNewInstance));
    Assert.assertFalse(hostSet.contains(liveInstances.get(0)));
    Assert.assertFalse(hostSet.contains(liveInstances.get(1)));

    // Test partitionAssignment host filter
    String payload2 = "{\"Options\" : { \"InstanceFilter\" : [\"" + liveInstances.get(0) + "\" , \""
        + liveInstances.get(1) + "\"] }}  ";
    String body2 = post(urlBase, null, Entity.entity(payload2, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode(), true);
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
        Response.Status.OK.getStatusCode(), true);
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
    String payload4 = "{\"InstanceChange\" : { \"AddInstances\" : [\" nonExistInstanceName \"] }} ";
    post(urlBase, null, Entity.entity(payload4, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode(), true);

    String payload5 =
        "{\"InstanceChange\" : { \"RemoveInstances\" : [\" nonExistInstanceName \"] }} ";
    post(urlBase, null, Entity.entity(payload5, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode(), true);

    String payload6 =
        "{\"InstanceChange\" : { \"SwapInstances\" : {\" nonExistInstanceName \" : \" swapNewInstance \"} }} ";
    post(urlBase, null, Entity.entity(payload6, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode(), true);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}