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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.rest.server.resources.helix.PerInstanceAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.util.InstanceValidationUtil;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestPerInstanceAccessor extends AbstractTestClass {
  private final static String CLUSTER_NAME = "TestCluster_0";
  private final static String INSTANCE_NAME = CLUSTER_NAME + "localhost_12918";

  @Test
  public void testIsInstanceStoppable() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    Map<String, String> params = ImmutableMap.of("client", "espresso");
    Entity entity =
        Entity.entity(OBJECT_MAPPER.writeValueAsString(params), MediaType.APPLICATION_JSON_TYPE);
    Response response = new JerseyUriRequestBuilder("clusters/{}/instances/{}/stoppable")
        .format(STOPPABLE_CLUSTER, "instance1").post(this, entity);
    String stoppableCheckResult = response.readEntity(String.class);
    Assert.assertEquals(stoppableCheckResult,
        "{\"stoppable\":false,\"failedChecks\":[\"Helix:EMPTY_RESOURCE_ASSIGNMENT\","
            + "\"Helix:INSTANCE_NOT_ENABLED\",\"Helix:INSTANCE_NOT_STABLE\"]}");
  }

  @Test (dependsOnMethods = "testIsInstanceStoppable")
  public void testGetAllMessages() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

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
    helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().message(INSTANCE_NAME, messageId),
        message);

    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages")
        .isBodyReturnExpected(true).format(CLUSTER_NAME, INSTANCE_NAME).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    int newMessageCount =
        node.get(PerInstanceAccessor.PerInstanceProperties.total_message_count.name()).getIntValue();

    Assert.assertEquals(newMessageCount, 1);
  }

  @Test(dependsOnMethods = "testGetAllMessages")
  public void testGetMessagesByStateModelDef() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

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
    helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().message(INSTANCE_NAME, messageId),
        message);

    String body =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages?stateModelDef=MasterSlave")
            .isBodyReturnExpected(true).format(CLUSTER_NAME, INSTANCE_NAME).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    int newMessageCount =
        node.get(PerInstanceAccessor.PerInstanceProperties.total_message_count.name()).getIntValue();

    Assert.assertEquals(newMessageCount, 1);

    body =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}/messages?stateModelDef=LeaderStandBy")
            .isBodyReturnExpected(true).format(CLUSTER_NAME, INSTANCE_NAME).get(this);
    node = OBJECT_MAPPER.readTree(body);
    newMessageCount =
        node.get(PerInstanceAccessor.PerInstanceProperties.total_message_count.name()).getIntValue();

    Assert.assertEquals(newMessageCount, 0);
  }

  @Test(dependsOnMethods = "testGetMessagesByStateModelDef")
  public void testGetInstanceById() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String body = new JerseyUriRequestBuilder("clusters/{}/instances/{}").isBodyReturnExpected(true)
        .format(CLUSTER_NAME, INSTANCE_NAME).get(this);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    String instancesCfg = node.get(PerInstanceAccessor.PerInstanceProperties.config.name()).toString();
    Assert.assertNotNull(instancesCfg);
    boolean isHealth = node.get("health").getBooleanValue();
    Assert.assertFalse(isHealth);

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

    new JerseyUriRequestBuilder("clusters/{}/instances/{}").format(CLUSTER_NAME, INSTANCE_NAME)
        .put(this, entity);

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

  @Test(dependsOnMethods = "testGetInstanceById")
  public void updateInstance() throws IOException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    // Disable instance
    Entity entity = Entity.entity("", MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=disable")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertFalse(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());

    // Enable instance
    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=enable")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    Assert.assertTrue(
        _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME).getInstanceEnabled());

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

    entity = Entity.entity(
        OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(AbstractResource.Properties.id.name(),
            INSTANCE_NAME, PerInstanceAccessor.PerInstanceProperties.resource.name(),
            CLUSTER_NAME + dbName.substring(0, dbName.length() - 1),
            PerInstanceAccessor.PerInstanceProperties.partitions.name(), partitionsToDisable)),
        MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=disablePartitions")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(
        new HashSet<>(instanceConfig.getDisabledPartitionsMap()
            .get(CLUSTER_NAME + dbName.substring(0, dbName.length() - 1))),
        new HashSet<>(partitionsToDisable));
    entity = Entity.entity(OBJECT_MAPPER.writeValueAsString(ImmutableMap
        .of(AbstractResource.Properties.id.name(), INSTANCE_NAME,
            PerInstanceAccessor.PerInstanceProperties.resource.name(),
            CLUSTER_NAME + dbName.substring(0, dbName.length() - 1),
            PerInstanceAccessor.PerInstanceProperties.partitions.name(),
            ImmutableList.of(CLUSTER_NAME + dbName + "1"))), MediaType.APPLICATION_JSON_TYPE);

    new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=enablePartitions")
        .format(CLUSTER_NAME, INSTANCE_NAME).post(this, entity);

    instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_NAME);
    Assert.assertEquals(new HashSet<>(instanceConfig.getDisabledPartitionsMap()
            .get(CLUSTER_NAME + dbName.substring(0, dbName.length() - 1))),
        new HashSet<>(Arrays.asList(CLUSTER_NAME + dbName + "0", CLUSTER_NAME + dbName + "3")));
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
  }

  @Test(dependsOnMethods = "checkUpdateFails")
  public void testCustomizedChecks() {
    // TODO: This is fake testing. Only validate it returns true value of this function.
    // For future, we need test: 1. mock the input of per participant API result to test validate logic
    //                           2. mock the input of per partition API result to test the sibling
    //                              check logic
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    String instanceName = "TestInstance";
    Assert.assertTrue(InstanceValidationUtil
        .checkCustomizedHealthStatusForInstance(_configAccessor, CLUSTER_NAME, instanceName,
            Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP));
  }


}
