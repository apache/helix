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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;
import org.apache.helix.rest.server.resources.helix.PerInstanceAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPerInstanceAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_4";
  private final static String INSTANCE_NAME = CLUSTER_NAME + "localhost_12918";

  private MockParticipantManager _instanceToDisable;

  @BeforeClass
  public void beforeClass() {
    int indexToDisable = -1;
    for (int i = 0; i < _mockParticipantManagers.size(); i++) {
      if (_mockParticipantManagers.get(i).getInstanceName().equals(INSTANCE_NAME)) {
        indexToDisable = i;
        break;
      }
    }
    _instanceToDisable = _mockParticipantManagers.remove(indexToDisable);
  }

  @Test
  public void testIsInstanceStoppable() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, String> params = ImmutableMap.of("client", "espresso");
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(params), MediaType.APPLICATION_JSON_TYPE);
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/stoppable?skipHealthCheckCategories=CUSTOM_INSTANCE_CHECK,CUSTOM_PARTITION_CHECK").format(
        STOPPABLE_CLUSTER, "instance1").post(this, entity);
    String stoppableCheckResult = response.readEntity(String.class);
    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(stoppableCheckResult, Map.class);
    List<String> failedChecks =
        Arrays.asList("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE");
    Map<String, Object> expectedMap =
        ImmutableMap.of("stoppable", false, "failedChecks", failedChecks);
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testIsInstanceStoppable")
  public void testTakeInstanceNegInput() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    post("clusters/TestCluster_0/instances/instance1/takeInstance", null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode(), true);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceNegInput")
  public void testTakeInstanceNegInput2() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1").post(this, Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    List<String> errorMsg = Arrays.asList("Invalid input. Please provide at least one health check or operation.");
    Map<String, Object> expectedMap =
        ImmutableMap.of("successful", false, "messages", errorMsg, "operationResult", "");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceNegInput2")
  public void testTakeInstanceHealthCheck() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"health_check_list\" : [\"HelixInstanceStoppableCheck\", \"CustomInstanceStoppableCheck\"],"
        + "\"health_check_config\" : { \"client\" : \"espresso\" }} ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1").post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    List<String> errorMsg = Arrays
        .asList("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE");
    Map<String, Object> expectedMap =
        ImmutableMap.of("successful", false, "messages", errorMsg, "operationResult", "");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceNegInput2")
  public void testTakeInstanceNonBlockingCheck() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"health_check_list\" : [\"HelixInstanceStoppableCheck\"],"
        + "\"health_check_config\" : { \"client\" : \"espresso\" , "
        + "\"continueOnFailures\" : [\"HELIX:EMPTY_RESOURCE_ASSIGNMENT\", \"HELIX:INSTANCE_NOT_ENABLED\","
        + " \"HELIX:INSTANCE_NOT_STABLE\"]} } ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1").post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    List<String> errorMsg = Arrays
        .asList("HELIX:EMPTY_RESOURCE_ASSIGNMENT", "HELIX:INSTANCE_NOT_ENABLED",
            "HELIX:INSTANCE_NOT_STABLE");
    Map<String, Object> expectedMap =
        ImmutableMap.of("successful", true, "messages", errorMsg, "operationResult", "");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceHealthCheck")
  public void testTakeInstanceOperationSuccess() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload =
        "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"]} ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Map<String, Object> expectedMap = ImmutableMap
        .of("successful", true, "messages", new ArrayList<>(), "operationResult", "DummyTakeOperationResult");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceOperationSuccess")
  public void testFreeInstanceOperationSuccess() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload =
        "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"]} ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/freeInstance")
        .format(STOPPABLE_CLUSTER, "instance1")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Map<String, Object> expectedMap = ImmutableMap
        .of("successful", true, "messages", new ArrayList<>(), "operationResult",
            "DummyFreeOperationResult");
    Assert.assertEquals(actualMap, expectedMap);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testFreeInstanceOperationSuccess")
  public void testTakeInstanceOperationCheckFailure() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"],"
        + "\"operation_config\": { \"org.apache.helix.rest.server.TestOperationImpl\" :"
        + " {\"instance0\": true, \"instance2\": true, "
        + "\"instance3\": true, \"instance4\": true, \"instance5\": true, "
        + " \"value\" : \"i001\", \"list_value\" : [\"list1\"]}} } ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance0")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Assert.assertFalse((boolean)actualMap.get("successful"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceOperationCheckFailure")
  public void testTakeInstanceOperationCheckFailureCommonInput() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"],"
        + "\"operation_config\": { \"OperationConfigSharedInput\" :"
        + " {\"instance0\": true, \"instance2\": true, "
        + "\"instance3\": true, \"instance4\": true, \"instance5\": true, "
        + " \"value\" : \"i001\", \"list_value\" : [\"list1\"]}}} ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance0")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Assert.assertFalse((boolean)actualMap.get("successful"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceOperationCheckFailureCommonInput")
  public void testTakeInstanceOperationCheckFailureNonBlocking() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"],"
        + "\"operation_config\": { \"org.apache.helix.rest.server.TestOperationImpl\" : "
        + "{\"instance0\": true, \"instance2\": true, "
        + "\"instance3\": true, \"instance4\": true, \"instance5\": true, "
        + "\"continueOnFailures\" : true} } } ";

    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance0")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);
    System.out.println("testTakeInstanceOperationCheckFailureNonBlocking" + takeInstanceResult);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Assert.assertTrue((boolean)actualMap.get("successful"));
    Assert.assertEquals(actualMap.get("operationResult"), "DummyTakeOperationResult");
    // The non blocking test should generate msg but won't return failure status
    Assert.assertFalse(actualMap.get("messages").equals("[]"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceOperationCheckFailureNonBlocking")
  public void testTakeInstanceCheckOnly() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String payload = "{ \"operation_list\" : [\"org.apache.helix.rest.server.TestOperationImpl\"],"
        + "\"operation_config\": {\"performOperation\": false} } ";
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/takeInstance")
        .format(STOPPABLE_CLUSTER, "instance1")
        .post(this, Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
    String takeInstanceResult = response.readEntity(String.class);

    Map<String, Object> actualMap = OBJECT_MAPPER.readValue(takeInstanceResult, Map.class);
    Assert.assertTrue((boolean)actualMap.get("successful"));
    Assert.assertTrue(actualMap.get("operationResult").equals(""));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testTakeInstanceCheckOnly")
  public void testGetAllMessages() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    _instanceToDisable.disconnect();

    String testInstance = INSTANCE_NAME; //Non-live instance

    String messageId = "msg1";
    Message message = new Message(Message.MessageType.STATE_TRANSITION, messageId);
    message.setStateModelDef("MasterSlave");
    message.setFromState("OFFLINE");
    message.setToState("SLAVE");
    message.setResourceName("testResourceName");
    message.setPartitionName("testResourceName_1");
    message.setTgtName("localhost_3");
    message.setTgtSessionId("session_3");
    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().message(testInstance, messageId), message);

    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages").isBodyReturnExpected(true).format(CLUSTER_NAME, testInstance).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    int newMessageCount =
        node.get(PerInstanceAccessor.PerInstanceProperties.total_message_count.name()).intValue();

    Assert.assertEquals(newMessageCount, 1);
    helixDataAccessor.removeProperty(helixDataAccessor.keyBuilder().message(testInstance, messageId));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetAllMessages")
  public void testGetMessagesByStateModelDef() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String testInstance = INSTANCE_NAME; //Non-live instance
    String messageId = "msg1";
    Message message = new Message(Message.MessageType.STATE_TRANSITION, messageId);
    message.setStateModelDef("MasterSlave");
    message.setFromState("OFFLINE");
    message.setToState("SLAVE");
    message.setResourceName("testResourceName");
    message.setPartitionName("testResourceName_1");
    message.setTgtName("localhost_3");
    message.setTgtSessionId("session_3");
    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().message(testInstance, messageId),
        message);

    String body =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages?stateModelDef=MasterSlave")
            .isBodyReturnExpected(true).format(CLUSTER_NAME, testInstance).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    int newMessageCount =
        node.get(PerInstanceAccessor.PerInstanceProperties.total_message_count.name()).intValue();

    Assert.assertEquals(newMessageCount, 1);

    body =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages?stateModelDef=LeaderStandBy")
            .isBodyReturnExpected(true).format(CLUSTER_NAME, testInstance).get(this);
    node = OBJECT_MAPPER.readTree(body);
    newMessageCount =
        node.get(PerInstanceAccessor.PerInstanceProperties.total_message_count.name()).intValue();

    Assert.assertEquals(newMessageCount, 0);
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, INSTANCE_NAME);
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, MockTask::new);
    StateMachineEngine stateMachineEngine = participant.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory("Task",
        new TaskStateModelFactory(participant, taskFactoryReg));
    participant.syncStart();
    _mockParticipantManagers.add(participant);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetMessagesByStateModelDef")
  public void testGetAllInstances() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances").isBodyReturnExpected(true)
        .format(CLUSTER_NAME).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesStr = node.get(InstancesAccessor.InstancesProperties.instances.name()).toString();
    Assert.assertNotNull(instancesStr);

    Set<String> instances = OBJECT_MAPPER.readValue(instancesStr,
        OBJECT_MAPPER.getTypeFactory().constructCollectionType(Set.class, String.class));
    String errorMessage = "Instances from response: "+ instances + " vs instances actually: "
        + _instancesMap.get(CLUSTER_NAME);
    Assert.assertEquals(instances.size(), _instancesMap.get(CLUSTER_NAME).size(), errorMessage);
    Assert.assertTrue(instances.containsAll(_instancesMap.get(CLUSTER_NAME)), errorMessage);
    Assert.assertTrue(_instancesMap.get(CLUSTER_NAME).containsAll(instances), errorMessage);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetAllInstances")
  public void testGetInstanceById() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}").isBodyReturnExpected(true)
        .format(CLUSTER_NAME, INSTANCE_NAME).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesCfg = node.get(PerInstanceAccessor.PerInstanceProperties.config.name()).toString();
    Assert.assertNotNull(instancesCfg);
    boolean isHealth = node.get("health").booleanValue();
    Assert.assertFalse(isHealth);

    InstanceConfig instanceConfig = new InstanceConfig(toZNRecord(instancesCfg));
    Assert.assertEquals(instanceConfig,
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testGetInstanceById")
  public void testAddInstance() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE_NAME + "TEST");
    Entity entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(instanceConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}").format(CLUSTER_NAME, INSTANCE_NAME)
        .put(this, entity);

    Assert.assertEquals(instanceConfig,
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME + "TEST"));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testAddInstance", expectedExceptions = HelixException.class)
  public void testDeleteInstance() {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    delete("clusters/" + CLUSTER_NAME + "/instances/" + INSTANCE_NAME + "TEST",
        Response.Status.OK.getStatusCode());
    _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME + "TEST");
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testDeleteInstance")
  public void updateInstance() throws Exception {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Disable instance
    Entity entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}?command=disable&instanceDisabledReason=reason1")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertFalse(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceDisabledType(),
        InstanceConstants.InstanceDisabledType.DEFAULT_INSTANCE_DISABLE_TYPE.toString());
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceDisabledReason(),
        "reason1");

    // Enable instance
    new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}?command=enable&instanceDisabledType=USER_OPERATION&instanceDisabledReason=reason1")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    Assert.assertTrue(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceDisabledType(),
        InstanceConstants.INSTANCE_NOT_DISABLED);
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceDisabledReason(),
        "");

    // We should see no instance disable related field in to clusterConfig
    ClusterConfig cls = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertFalse(cls.getRecord().getMapFields()
        .containsKey(ClusterConfig.ClusterConfigProperty.DISABLED_INSTANCES.name()));

    // disable instance with no reason input
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=disable")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertFalse(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=enable")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    Assert.assertTrue(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());

    // Disable instance should see no field write to clusterConfig
    cls = _configAccessor.getClusterConfig(CLUSTER_NAME);
    Assert.assertFalse(cls.getRecord().getMapFields()
        .containsKey(ClusterConfig.ClusterConfigProperty.DISABLED_INSTANCES.name()));

    // AddTags
    List<String> tagList = ImmutableList.of("tag3", "tag1", "tag2");
    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(AbstractResource.Properties.id.name(),
            INSTANCE_NAME, PerInstanceAccessor.PerInstanceProperties.instanceTags.name(), tagList)),
        MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=addInstanceTag")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertEquals(_configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getTags(),
        tagList);

    // RemoveTags
    List<String> removeList = new ArrayList<>(tagList);
    removeList.remove("tag2");
    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(AbstractResource.Properties.id.name(),
            INSTANCE_NAME, PerInstanceAccessor.PerInstanceProperties.instanceTags.name(), removeList)),
        MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=removeInstanceTag")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertEquals(_configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getTags(),
        ImmutableList.of("tag2"));

    // Test enable disable partitions
    String dbName = "_db_0_";
    List<String> partitionsToDisable = Arrays.asList(CLUSTER_NAME + dbName + "0",
        CLUSTER_NAME + dbName + "1", CLUSTER_NAME + dbName + "3");
    String RESOURCE_NAME = CLUSTER_NAME + dbName.substring(0, dbName.length() - 1);

    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(AbstractResource.Properties.id.name(),
            INSTANCE_NAME, PerInstanceAccessor.PerInstanceProperties.resource.name(), RESOURCE_NAME,
            PerInstanceAccessor.PerInstanceProperties.partitions.name(), partitionsToDisable)),
        MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=disablePartitions")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(
        new HashSet<>(instanceConfig.getDisabledPartitionsMap().get(RESOURCE_NAME)),
        new HashSet<>(partitionsToDisable));
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(ImmutableMap
        .of(AbstractResource.Properties.id.name(), INSTANCE_NAME,
            PerInstanceAccessor.PerInstanceProperties.resource.name(), RESOURCE_NAME,
            PerInstanceAccessor.PerInstanceProperties.partitions.name(),
            ImmutableList.of(CLUSTER_NAME + dbName + "1"))), MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=enablePartitions")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(new HashSet<>(instanceConfig.getDisabledPartitionsMap().get(RESOURCE_NAME)),
        new HashSet<>(Arrays.asList(CLUSTER_NAME + dbName + "0", CLUSTER_NAME + dbName + "3")));

    // test set instance operation
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=EVACUATE")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=INVALIDOP")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE);

    // test canCompleteSwap
    Response canCompleteSwapResponse =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=canCompleteSwap").format(
            CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    Assert.assertEquals(canCompleteSwapResponse.getStatus(), Response.Status.OK.getStatusCode());
    Map<String, Object> responseMap =
        OBJECT_MAPPER.readValue(canCompleteSwapResponse.readEntity(String.class), Map.class);
    Assert.assertFalse((boolean) responseMap.get("successful"));

    // test completeSwapIfPossible
    Response completeSwapIfPossibleResponse = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}?command=completeSwapIfPossible").format(CLUSTER_NAME,
        INSTANCE_NAME).post(this, entity);
    Assert.assertEquals(completeSwapIfPossibleResponse.getStatus(),
        Response.Status.OK.getStatusCode());
    responseMap =
        OBJECT_MAPPER.readValue(completeSwapIfPossibleResponse.readEntity(String.class), Map.class);
    Assert.assertFalse((boolean) responseMap.get("successful"));

    // test isEvacuateFinished on instance with EVACUATE but has currentState
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=EVACUATE")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);

    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=isEvacuateFinished")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    Map<String, Boolean> evacuateFinishedResult = OBJECT_MAPPER.readValue(response.readEntity(String.class), Map.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    // Returns true because the node only contains semi-auto resources
    Assert.assertTrue(evacuateFinishedResult.get("successful"));

    // Because the resources are now all semi-auto, is EvacuateFinished should return true
    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=isEvacuateFinished")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    evacuateFinishedResult = OBJECT_MAPPER.readValue(response.readEntity(String.class), Map.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertTrue(evacuateFinishedResult.get("successful"));

    // test isEvacuateFinished on instance with EVACUATE and no currentState
    // Create new instance so no currentState or messages assigned to it
    String test_instance_name = INSTANCE_NAME + "_foo";
    InstanceConfig newInstanceConfig = new InstanceConfig(test_instance_name);
    Entity instanceEntity = Entity.entity(OBJECT_MAPPER.writeValueAsString(newInstanceConfig.getRecord()),
        MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}").format(CLUSTER_NAME, test_instance_name)
        .put(this, instanceEntity);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setInstanceOperation&instanceOperation=EVACUATE")
        .format(CLUSTER_NAME, test_instance_name).post(this, entity);
    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, test_instance_name);
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);

    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=isEvacuateFinished")
        .format(CLUSTER_NAME, test_instance_name).post(this, entity);
    evacuateFinishedResult = OBJECT_MAPPER.readValue(response.readEntity(String.class), Map.class);
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertTrue(evacuateFinishedResult.get("successful"));

    // Remove instance created for evacuate test
    delete("clusters/" + CLUSTER_NAME + "/instances/" + test_instance_name, Response.Status.OK.getStatusCode());

    // test setPartitionsToError
    List<String> partitionsToSetToError = Arrays.asList(CLUSTER_NAME + dbName + "7");

    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(AbstractResource.Properties.id.name(),
            INSTANCE_NAME, PerInstanceAccessor.PerInstanceProperties.resource.name(), RESOURCE_NAME,
            PerInstanceAccessor.PerInstanceProperties.partitions.name(), partitionsToSetToError)),
        MediaType.APPLICATION_JSON_TYPE);

    response = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=setPartitionsToError")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

    TestHelper.verify(() -> {
      ExternalView externalView = _gSetupTool.getClusterManagementTool()
          .getResourceExternalView(CLUSTER_NAME, RESOURCE_NAME);
      Set responseForAllPartitions = new HashSet();
      for (String partition : partitionsToSetToError) {
        responseForAllPartitions.add(externalView.getStateMap(partition)
            .get(INSTANCE_NAME) == HelixDefinedState.ERROR.toString());
      }
      return !responseForAllPartitions.contains(Boolean.FALSE);
    }, TestHelper.WAIT_DURATION);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test "update" command for updateInstanceConfig endpoint.
   * @throws IOException
   */
  @Test(dependsOnMethods = "updateInstance")
  public void updateInstanceConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceName = CLUSTER_NAME + "localhost_12918";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);
    ZNRecord record = instanceConfig.getRecord();

    // Generate a record containing three keys (k0, k1, k2) for all fields
    String value = "value";
    for (int i = 0; i < 3; i++) {
      String key = "k" + i;
      record.getSimpleFields().put(key, value);
      record.getMapFields().put(key, ImmutableMap.of(key, value));
      record.getListFields().put(key, Arrays.asList(key, value));
    }

    // 1. Add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=update")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    // Check that the fields have been added
    Assert.assertEquals(record.getSimpleFields(), _configAccessor
        .getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getSimpleFields());
    Assert.assertEquals(record.getListFields(),
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getListFields());
    Assert.assertEquals(record.getMapFields(),
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getMapFields());

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
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=update")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    // Check that the fields have been modified
    Assert.assertEquals(record.getSimpleFields(), _configAccessor
        .getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getSimpleFields());
    Assert.assertEquals(record.getListFields(),
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getListFields());
    Assert.assertEquals(record.getMapFields(),
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord().getMapFields());
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test the "delete" command of updateInstanceConfig.
   * @throws IOException
   */
  @Test(dependsOnMethods = "updateInstanceConfig")
  public void deleteInstanceConfig() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceName = CLUSTER_NAME + "localhost_12918";
    ZNRecord record = new ZNRecord(instanceName);

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
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs?command=delete")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    // Check that the keys k1 and k2 have been deleted, and k0 remains
    for (int i = 0; i < 4; i++) {
      String key = "k" + i;
      if (i == 0) {
        Assert.assertTrue(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
            .getSimpleFields().containsKey(key));
        Assert.assertTrue(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
            .getListFields().containsKey(key));
        Assert.assertTrue(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
            .getMapFields().containsKey(key));
        continue;
      }
      Assert.assertFalse(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
          .getSimpleFields().containsKey(key));
      Assert.assertFalse(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
          .getListFields().containsKey(key));
      Assert.assertFalse(_configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getRecord()
          .getMapFields().containsKey(key));
    }
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Check that updateInstanceConfig fails when there is no pre-existing InstanceConfig ZNode. This
   * is because InstanceConfig should have been created when the instance was added, and this REST
   * endpoint is not meant for creation.
   */
  @Test(dependsOnMethods = "deleteInstanceConfig")
  public void checkUpdateFails() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceName = CLUSTER_NAME + "non_existent_instance";
    InstanceConfig instanceConfig = new InstanceConfig(INSTANCE_NAME + "TEST");
    ZNRecord record = instanceConfig.getRecord();
    record.getSimpleFields().put("TestSimple", "value");
    record.getMapFields().put("TestMap", ImmutableMap.of("key", "value"));
    record.getListFields().put("TestList", Arrays.asList("e1", "e2", "e3"));

    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}/configs")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode())
        .format(CLUSTER_NAME, instanceName).post(this, entity);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Check that validateWeightForInstance() works by
   * 1. First call validate -> We should get "true" because nothing is set in ClusterConfig.
   * 2. Define keys in ClusterConfig and call validate -> We should get BadRequest.
   * 3. Define weight configs in InstanceConfig and call validate -> We should get OK with "true".
   */
  @Test(dependsOnMethods = "checkUpdateFails")
  public void testValidateWeightForInstance()
      throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Empty out ClusterConfig's weight key setting and InstanceConfig's capacity maps for testing
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord()
        .setListField(ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name(),
            new ArrayList<>());
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    List<String> instances =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME);
    for (String instance : instances) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setInstanceCapacityMap(Collections.emptyMap());
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }

    // Get one instance in the cluster
    String selectedInstance =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME).iterator()
            .next();

    // Issue a validate call
    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, selectedInstance).get(this);

    JsonNode node = OBJECT_MAPPER.readTree(body);
    // Must have the result saying (true) because there's no capacity keys set
    // in ClusterConfig
    node.iterator().forEachRemaining(child -> Assert.assertTrue(child.booleanValue()));

    // Define keys in ClusterConfig
    clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    body = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, selectedInstance)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode()).get(this);
    node = OBJECT_MAPPER.readTree(body);
    // Since instance does not have weight-related configs, the result should return error
    Assert.assertTrue(node.has("error"));

    // Now set weight-related config in InstanceConfig
    InstanceConfig instanceConfig =
        _configAccessor.getInstanceConfig(CLUSTER_NAME, selectedInstance);
    instanceConfig.setInstanceCapacityMap(ImmutableMap.of("FOO", 1000, "BAR", 1000));
    _configAccessor.setInstanceConfig(CLUSTER_NAME, selectedInstance, instanceConfig);

    body = new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=validateWeight")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, selectedInstance)
        .expectedReturnStatusCode(Response.Status.OK.getStatusCode()).get(this);
    node = OBJECT_MAPPER.readTree(body);
    // Must have the results saying they are all valid (true) because capacity keys are set
    // in ClusterConfig
    node.iterator().forEachRemaining(child -> Assert.assertTrue(child.booleanValue()));
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  /**
   * Test the sanity check when updating the instance config.
   * The config is validated at rest server side.
   */
  @Test(dependsOnMethods = "testValidateWeightForInstance")
  public void testValidateDeltaInstanceConfigForUpdate() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Enable Topology aware for the cluster
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord()
        .setListField(ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name(),
            new ArrayList<>());
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/Rack/Sub-Rack/Host/Instance");
    clusterConfig.setFaultZoneType("Host");
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    String instanceName = CLUSTER_NAME + "localhost_12918";
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);

    // Update InstanceConfig with Topology Info
    String domain = "Rack=rack1, Sub-Rack=Sub-Rack1, Host=Host-1";
    ZNRecord record = instanceConfig.getRecord();
    record.getSimpleFields().put(InstanceConfig.InstanceConfigProperty.DOMAIN.name(), domain);

    // Add these fields by way of "update"
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/configs?command=update&doSanityCheck=true")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);
    // Check that the fields have been added
    Assert.assertEquals(response.getStatus(), 200);
    // Check the cluster config is updated
    Assert.assertEquals(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName).getDomainAsString(), domain);

    // set domain to an invalid value
    record.getSimpleFields()
        .put(InstanceConfig.InstanceConfigProperty.DOMAIN.name(), "InvalidDomainValue");
    entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(record), MediaType.APPLICATION_JSON_TYPE);
    // Updating using an invalid domain value should return a non-OK response
    new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}/configs?command=update&doSanityCheck=true")
        .expectedReturnStatusCode(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testValidateDeltaInstanceConfigForUpdate")
  public void testGetResourcesOnInstance() throws JsonProcessingException, InterruptedException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}/resources")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, INSTANCE_NAME).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    ArrayNode arrayOfResource =
        (ArrayNode) node.get(PerInstanceAccessor.PerInstanceProperties.resources.name());
    Assert.assertTrue(arrayOfResource.size() != 0);
    String dbNameString= arrayOfResource.get(0).toString();
    String dbName = dbNameString.substring(1,dbNameString.length()-1);
    // The below calls should successfully return
    body = new JerseyUriRequestBuilder("clusters/{}/instances/{}/resources/{}")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, INSTANCE_NAME, dbName).get(this);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}
