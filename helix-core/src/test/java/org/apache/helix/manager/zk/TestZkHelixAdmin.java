package org.apache.helix.manager.zk;

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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.api.exceptions.HelixConflictException;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.api.status.ClusterManagementModeRequest;
import org.apache.helix.api.topology.ClusterTopology;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.examples.MasterSlaveStateModelFactory;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.zkclient.NetworkUtil;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZkHelixAdmin extends ZkTestBase {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeClass
  public void beforeClass() {
  }

  @Test()
  public void testZkHelixAdmin() {
    // TODO refactor this test into small test cases and use @before annotations
    System.out.println("START testZkHelixAdmin at " + new Date(System.currentTimeMillis()));

    final String clusterName = getShortClassName();
    String rootPath = "/" + clusterName;
    if (_gZkClient.exists(rootPath)) {
      _gZkClient.deleteRecursively(rootPath);
    }

    HelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient));
    Assert.assertTrue(_gZkClient.exists(PropertyPathBuilder.customizedStateConfig(clusterName)));
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient));
    Assert.assertTrue(_gZkClient.exists(PropertyPathBuilder.customizedStateConfig(clusterName)));

    List<String> list = tool.getClusters();
    AssertJUnit.assertTrue(list.size() > 0);

    try {
      Stat oldstat = _gZkClient.getStat(rootPath);
      Assert.assertNotNull(oldstat);
      boolean success = tool.addCluster(clusterName, false);
      // Even though it exists, it should return true but it should not make any changes in zk
      Assert.assertTrue(success);
      Stat newstat = _gZkClient.getStat(rootPath);
      Assert.assertEquals(oldstat, newstat);
    } catch (HelixException e) {
      // OK
    }

    String hostname = "host1";
    String port = "9999";
    String instanceName = hostname + "_" + port;
    InstanceConfig config = new InstanceConfig(instanceName);
    config.setHostName(hostname);
    config.setPort(port);
    List<String> dummyList = new ArrayList<>();
    dummyList.add("foo");
    dummyList.add("bar");
    config.getRecord().setListField("dummy", dummyList);
    tool.addInstance(clusterName, config);
    tool.enableInstance(clusterName, instanceName, true);
    String path = PropertyPathBuilder.getPath(PropertyType.INSTANCES, clusterName, instanceName);
    AssertJUnit.assertTrue(_gZkClient.exists(path));

    try {
      tool.addInstance(clusterName, config);
      Assert.fail("should fail if add an already-existing instance");
    } catch (HelixException e) {
      // OK
    }

    config = tool.getInstanceConfig(clusterName, instanceName);
    AssertJUnit.assertEquals(config.getId(), instanceName);

    // test setInstanceConfig()
    config = tool.getInstanceConfig(clusterName, instanceName);
    config.setHostName("host2");
    try {
      // different host
      tool.setInstanceConfig(clusterName, instanceName, config);
      Assert.fail("should fail if hostname is different from the current one");
    } catch (HelixException e) {
      // OK
    }

    config = tool.getInstanceConfig(clusterName, instanceName);
    config.setPort("7777");
    try {
      // different port
      tool.setInstanceConfig(clusterName, instanceName, config);
      Assert.fail("should fail if port is different from the current one");
    } catch (HelixException e) {
      // OK
    }

    Assert.assertTrue(
        tool.getInstanceConfig(clusterName, instanceName).getInstanceDisabledReason().isEmpty());
    String disableReason = "Reason";
    tool.enableInstance(clusterName, instanceName, false,
        InstanceConstants.InstanceDisabledType.CLOUD_EVENT, disableReason);
    Assert.assertTrue(tool.getInstanceConfig(clusterName, instanceName).getInstanceDisabledReason()
        .equals(disableReason));
    tool.enableInstance(clusterName, instanceName, true,
        InstanceConstants.InstanceDisabledType.CLOUD_EVENT, disableReason);
    Assert.assertTrue(
        tool.getInstanceConfig(clusterName, instanceName).getInstanceDisabledReason().isEmpty());
    Assert.assertEquals(tool.getInstanceConfig(clusterName, instanceName).getInstanceDisabledType(),
        InstanceConstants.INSTANCE_NOT_DISABLED);

    dummyList.remove("bar");
    dummyList.add("baz");
    config = tool.getInstanceConfig(clusterName, instanceName);
    config.getRecord().setListField("dummy", dummyList);
    AssertJUnit.assertTrue(tool.setInstanceConfig(clusterName, "host1_9999", config));
    config = tool.getInstanceConfig(clusterName, "host1_9999");
    dummyList = config.getRecord().getListField("dummy");
    AssertJUnit.assertTrue(dummyList.contains("foo"));
    AssertJUnit.assertTrue(dummyList.contains("baz"));
    AssertJUnit.assertFalse(dummyList.contains("bar"));
    AssertJUnit.assertEquals(2, dummyList.size());

    // test: should not drop instance when it is still alive
    HelixManager manager = initializeHelixManager(clusterName, config.getInstanceName());
    try {
      manager.connect();
    } catch (Exception e) {
      Assert.fail("HelixManager failed connecting");
    }

    try {
      tool.dropInstance(clusterName, config);
      Assert.fail("should fail if an instance is still alive");
    } catch (HelixException e) {
      // OK
    }

    try {
      manager.disconnect();
    } catch (Exception e) {
      Assert.fail("HelixManager failed disconnecting");
    }

    // Tests that ZkClientException thrown from ZkClient should be caught
    // and it should be converted HelixException to be rethrown
    String instancePath = PropertyPathBuilder.instance(clusterName, config.getInstanceName());
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
    String liveInstancePath = PropertyPathBuilder.liveInstance(clusterName, instanceName);
    RealmAwareZkClient mockZkClient = Mockito.mock(RealmAwareZkClient.class);
    // Mock the exists() method to let dropInstance() reach deleteRecursively().
    Mockito.when(mockZkClient.exists(instanceConfigPath)).thenReturn(true);
    Mockito.when(mockZkClient.exists(instancePath)).thenReturn(true);
    Mockito.when(mockZkClient.exists(liveInstancePath)).thenReturn(false);
    Mockito.doThrow(new ZkClientException("ZkClientException: failed to delete " + instancePath,
        new ZkException("ZkException: failed to delete " + instancePath,
            new KeeperException.NotEmptyException(
                "NotEmptyException: directory" + instancePath + " is not empty"))))
        .when(mockZkClient).deleteRecursively(instancePath);

    HelixAdmin helixAdminMock = new ZKHelixAdmin(mockZkClient);
    try {
      helixAdminMock.dropInstance(clusterName, config);
      Assert.fail("Should throw HelixException");
    } catch (HelixException expected) {
      // This exception is expected because it is converted from ZkClientException and rethrown.
      Assert.assertEquals(expected.getMessage(),
          "Failed to drop instance: " + config.getInstanceName() + ". Retry times: 3");
    } catch (ZkClientException e) {
      if (e.getMessage().equals("ZkClientException: failed to delete " + instancePath)) {
        Assert.fail("Should not throw ZkClientException because it should be caught.");
      }
    }

    tool.dropInstance(clusterName, config); // correctly drop the instance

    try {
      tool.getInstanceConfig(clusterName, "host1_9999");
      Assert.fail("should fail if get a non-existent instance");
    } catch (HelixException e) {
      // OK
    }
    try {
      tool.dropInstance(clusterName, config);
      Assert.fail("should fail if drop on a non-existent instance");
    } catch (HelixException e) {
      // OK
    }
    try {
      tool.enableInstance(clusterName, "host1_9999", false);
      Assert.fail("should fail if enable a non-existent instance");
    } catch (HelixException e) {
      // OK
    }

    ZNRecord stateModelRecord = new ZNRecord("id1");
    try {
      tool.addStateModelDef(clusterName, "id1", new StateModelDefinition(stateModelRecord));
      path = PropertyPathBuilder.stateModelDef(clusterName, "id1");
      AssertJUnit.assertTrue(_gZkClient.exists(path));
      Assert.fail("should fail");
    } catch (HelixException | IllegalArgumentException e) {
      // OK
    }

    tool.addStateModelDef(clusterName, "MasterSlave",
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave()));
    stateModelRecord = StateModelConfigGenerator.generateConfigForMasterSlave();
    tool.addStateModelDef(clusterName, stateModelRecord.getId(),
        new StateModelDefinition(stateModelRecord));
    list = tool.getStateModelDefs(clusterName);
    AssertJUnit.assertEquals(list.size(), 1);

    try {
      tool.addResource(clusterName, "resource", 10, "nonexistStateModelDef");
      Assert.fail("should fail if add a resource without an existing state model");
    } catch (HelixException e) {
      // OK
    }
    try {
      tool.addResource(clusterName, "resource", 10, "id1");
      Assert.fail("should fail");
    } catch (HelixException e) {
      // OK
    }
    list = tool.getResourcesInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 0);
    try {
      tool.addResource(clusterName, "resource", 10, "id1");
      Assert.fail("should fail");
    } catch (HelixException e) {
      // OK
    }
    list = tool.getResourcesInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 0);

    ExternalView resourceExternalView = tool.getResourceExternalView(clusterName, "resource");
    AssertJUnit.assertNull(resourceExternalView);

    CustomizedView resourceCustomizedView = tool.getResourceCustomizedView(clusterName,"resource"
        , "customizedStateType");
    AssertJUnit.assertNull(resourceCustomizedView);

    // test config support
    // ConfigScope scope = new ConfigScopeBuilder().forCluster(clusterName)
    // .forResource("testResource").forPartition("testPartition").build();
    HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.PARTITION)
        .forCluster(clusterName).forResource("testResource").forPartition("testPartition").build();

    Map<String, String> properties = new HashMap<>();
    properties.put("pKey1", "pValue1");
    properties.put("pKey2", "pValue2");

    // make sure calling set/getConfig() many times will not drain zkClient resources
    // int nbOfZkClients = ZkClient.getNumberOfConnections();
    for (int i = 0; i < 100; i++) {
      tool.setConfig(scope, properties);
      Map<String, String> newProperties =
          tool.getConfig(scope, new ArrayList<>(properties.keySet()));
      Assert.assertEquals(newProperties.size(), 2);
      Assert.assertEquals(newProperties.get("pKey1"), "pValue1");
      Assert.assertEquals(newProperties.get("pKey2"), "pValue2");
    }

    deleteCluster(clusterName);
    System.out.println("END testZkHelixAdmin at " + new Date(System.currentTimeMillis()));
  }

  private HelixManager initializeHelixManager(String clusterName, String instanceName) {
    HelixManager manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName,
        InstanceType.PARTICIPANT, org.apache.helix.common.ZkTestBase.ZK_ADDR);

    MasterSlaveStateModelFactory stateModelFactory = new MasterSlaveStateModelFactory(instanceName);

    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory("id1", stateModelFactory);
    return manager;
  }

  // drop resource should drop corresponding resource-level config also
  @Test
  public void testDropResource() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient), "Cluster should be setup");

    tool.addStateModelDef(clusterName, "MasterSlave",
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave()));
    tool.addResource(clusterName, "test-db", 4, "MasterSlave");
    Map<String, String> resourceConfig = new HashMap<>();
    resourceConfig.put("key1", "value1");
    tool.setConfig(new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(clusterName)
        .forResource("test-db").build(), resourceConfig);

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    Assert.assertTrue(_gZkClient.exists(keyBuilder.idealStates("test-db").getPath()),
        "test-db ideal-state should exist");
    Assert.assertTrue(_gZkClient.exists(keyBuilder.resourceConfig("test-db").getPath()),
        "test-db resource config should exist");

    tool.dropResource(clusterName, "test-db");
    Assert.assertFalse(_gZkClient.exists(keyBuilder.idealStates("test-db").getPath()),
        "test-db ideal-state should be dropped");
    Assert.assertFalse(_gZkClient.exists(keyBuilder.resourceConfig("test-db").getPath()),
        "test-db resource config should be dropped");

    tool.dropCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  // test add/remove message constraint
  @Test
  public void testAddRemoveMsgConstraint() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient), "Cluster should be setup");

    // test admin.getMessageConstraints()
    ClusterConstraints constraints =
        tool.getConstraints(clusterName, ConstraintType.MESSAGE_CONSTRAINT);
    Assert.assertNull(constraints, "message-constraint should NOT exist for cluster: " + className);

    // remove non-exist constraint
    try {
      tool.removeConstraint(clusterName, ConstraintType.MESSAGE_CONSTRAINT, "constraint1");
      // will leave a null message-constraint znode on zk
    } catch (Exception e) {
      Assert.fail("Should not throw exception when remove a non-exist constraint.");
    }

    // add a message constraint
    ConstraintItemBuilder builder = new ConstraintItemBuilder();
    builder.addConstraintAttribute(ConstraintAttribute.RESOURCE.toString(), "MyDB")
        .addConstraintAttribute(ConstraintAttribute.CONSTRAINT_VALUE.toString(), "1");
    tool.setConstraint(clusterName, ConstraintType.MESSAGE_CONSTRAINT, "constraint1",
        builder.build());

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    constraints =
        accessor.getProperty(keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()));
    Assert.assertNotNull(constraints, "message-constraint should exist");
    ConstraintItem item = constraints.getConstraintItem("constraint1");
    Assert.assertNotNull(item, "message-constraint for constraint1 should exist");
    Assert.assertEquals(item.getConstraintValue(), "1");
    Assert.assertEquals(item.getAttributeValue(ConstraintAttribute.RESOURCE), "MyDB");

    // test admin.getMessageConstraints()
    constraints = tool.getConstraints(clusterName, ConstraintType.MESSAGE_CONSTRAINT);
    Assert.assertNotNull(constraints, "message-constraint should exist");
    item = constraints.getConstraintItem("constraint1");
    Assert.assertNotNull(item, "message-constraint for constraint1 should exist");
    Assert.assertEquals(item.getConstraintValue(), "1");
    Assert.assertEquals(item.getAttributeValue(ConstraintAttribute.RESOURCE), "MyDB");

    // remove a exist message-constraint
    tool.removeConstraint(clusterName, ConstraintType.MESSAGE_CONSTRAINT, "constraint1");
    constraints =
        accessor.getProperty(keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()));
    Assert.assertNotNull(constraints, "message-constraint should exist");
    item = constraints.getConstraintItem("constraint1");
    Assert.assertNull(item, "message-constraint for constraint1 should NOT exist");

    tool.dropCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDisableResource() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient), "Cluster should be setup");
    String resourceName = "TestDB";
    admin.addStateModelDef(clusterName, "MasterSlave",
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave()));
    admin.addResource(clusterName, resourceName, 4, "MasterSlave");
    admin.enableResource(clusterName, resourceName, false);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    Assert.assertFalse(idealState.isEnabled());
    admin.enableResource(clusterName, resourceName, true);
    idealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    Assert.assertTrue(idealState.isEnabled());

    admin.dropCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testGetResourcesWithTag() {
    String TEST_TAG = "TestTAG";

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    HelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient));

    tool.addStateModelDef(clusterName, "OnlineOffline",
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));

    for (int i = 0; i < 4; i++) {
      String instanceName = "host" + i + "_9999";
      InstanceConfig config = new InstanceConfig(instanceName);
      config.setHostName("host" + i);
      config.setPort("9999");
      // set tag to two instances
      if (i < 2) {
        config.addTag(TEST_TAG);
      }
      tool.addInstance(clusterName, config);
      tool.enableInstance(clusterName, instanceName, true);
      String path = PropertyPathBuilder.instance(clusterName, instanceName);
      AssertJUnit.assertTrue(_gZkClient.exists(path));
    }

    for (int i = 0; i < 4; i++) {
      String resourceName = "database_" + i;
      IdealState is = new IdealState(resourceName);
      is.setStateModelDefRef("OnlineOffline");
      is.setNumPartitions(2);
      is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      is.setReplicas("1");
      is.enable(true);
      if (i < 2) {
        is.setInstanceGroupTag(TEST_TAG);
      }
      tool.addResource(clusterName, resourceName, is);
    }

    List<String> allResources = tool.getResourcesInCluster(clusterName);
    List<String> resourcesWithTag = tool.getResourcesInClusterWithTag(clusterName, TEST_TAG);
    AssertJUnit.assertEquals(allResources.size(), 4);
    AssertJUnit.assertEquals(resourcesWithTag.size(), 2);

    tool.dropCluster(clusterName);
  }

  @Test
  public void testEnableDisablePartitions() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String instanceName = "TestInstance";
    String testResourcePrefix = "TestResource";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName, true);
    admin.addInstance(clusterName, new InstanceConfig(instanceName));

    // Test disable instances with resources
    admin.enablePartition(false, clusterName, instanceName, testResourcePrefix + "0",
        Arrays.asList("1", "2"));
    admin.enablePartition(false, clusterName, instanceName, testResourcePrefix + "1",
        Arrays.asList("2", "3", "4"));
    InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "0").size(), 2);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "1").size(), 3);

    // Test disable partition across resources
    // TODO : Remove this part once setInstanceEnabledForPartition(partition, enabled) is removed
    instanceConfig.setInstanceEnabledForPartition("10", false);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "0").size(), 3);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "1").size(), 4);
    admin.dropCluster(clusterName);
  }

  @Test
  public void testLegacyEnableDisablePartition() {
    String instanceName = "TestInstanceLegacy";
    String testResourcePrefix = "TestResourceLegacy";
    ZNRecord record = new ZNRecord(instanceName);
    List<String> disabledPartitions = new ArrayList<>(Arrays.asList("1", "2", "3"));
    record.setListField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name(),
        disabledPartitions);
    InstanceConfig instanceConfig = new InstanceConfig(record);
    instanceConfig.setInstanceEnabledForPartition(testResourcePrefix, "2", false);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix).size(), 3);
    Assert.assertEquals(instanceConfig.getRecord()
            .getListField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name()).size(),
        3);
    instanceConfig.setInstanceEnabledForPartition(testResourcePrefix, "2", true);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix).size(), 2);
    Assert.assertEquals(instanceConfig.getRecord()
            .getListField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name()).size(),
        2);
  }

  @Test
  public void testResetPartition() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String instanceName = "TestInstance";
    String testResource = "TestResource";
    String wrongTestInstance = "WrongTestInstance";
    String wrongTestResource = "WrongTestResource";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName, true);
    admin.addInstance(clusterName, new InstanceConfig(instanceName));
    admin.enableInstance(clusterName, instanceName, true);
    InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);

    IdealState idealState = new IdealState(testResource);
    idealState.setNumPartitions(3);
    admin.addStateModelDef(clusterName, "MasterSlave", new MasterSlaveSMD());
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    admin.addResource(clusterName, testResource, idealState);
    admin.enableResource(clusterName, testResource, true);

    /*
     * This is a unit test for sanity check in resetPartition().
     * There is no running controller in this test. We have end-to-end tests for resetPartition()
     * under webapp/TestResetPartitionState and integration/TestResetPartitionState.
     */
    // resetPartition is expected to throw an exception when provided with a nonexistent instance.
    try {
      admin.resetPartition(clusterName, wrongTestInstance, testResource, Arrays.asList("1", "2"));
      Assert.fail("Should throw HelixException");
    } catch (HelixException expected) {
      // This exception is expected because the instance name is made up.
      Assert.assertEquals(expected.getMessage(), String.format(
          "Can't reset state for %s.[1, 2] on WrongTestInstance, because %s does not exist in cluster %s",
          testResource, wrongTestInstance, clusterName));
    }

    // resetPartition is expected to throw an exception when provided with a non-live instance.
    try {
      admin.resetPartition(clusterName, instanceName, testResource, Arrays.asList("1", "2"));
      Assert.fail("Should throw HelixException");
    } catch (HelixException expected) {
      // This exception is expected because the instance is not alive.
      Assert.assertEquals(expected.getMessage(), String
          .format("Can't reset state for %s.[1, 2] on %s, because %s is not alive in cluster %s",
              testResource, instanceName, instanceName, clusterName));
    }

    HelixManager manager = initializeHelixManager(clusterName, instanceConfig.getInstanceName());
    manager.connect();

    // resetPartition is expected to throw an exception when provided with a nonexistent resource.
    try {
      admin.resetPartition(clusterName, instanceName, wrongTestResource, Arrays.asList("1", "2"));
      Assert.fail("Should throw HelixException");
    } catch (HelixException expected) {
      // This exception is expected because the resource is not added.
      Assert.assertEquals(expected.getMessage(), String.format(
          "Can't reset state for %s.[1, 2] on %s, because resource %s is not added to cluster %s",
          wrongTestResource, instanceName, wrongTestResource, clusterName));
    }

    try {
      admin.resetPartition(clusterName, instanceName, testResource, Arrays.asList("1", "2"));
      Assert.fail("Should throw HelixException");
    } catch (HelixException expected) {
      // This exception is expected because partitions do not exist.
      Assert.assertEquals(expected.getMessage(), String.format(
          "Can't reset state for %s.[1, 2] on %s, because not all [1, 2] exist in cluster %s",
          testResource, instanceName, clusterName));
    }

    // clean up
    manager.disconnect();
    admin.dropCluster(clusterName);

    // verify the cluster has been removed successfully
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(className, new ZkBaseDataAccessor<>(_gZkClient));
    try {
      Assert.assertTrue(TestHelper.verify(() -> dataAccessor.getChildNames(dataAccessor.keyBuilder().liveInstances()).isEmpty(), 1000));
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("There're live instances not cleaned up yet");
      assert false;
    }

    try {
      Assert.assertTrue(TestHelper.verify(() -> dataAccessor.getChildNames(dataAccessor.keyBuilder().clusterConfig()).isEmpty(), 1000));
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("The cluster is not cleaned up yet");
      assert false;
    }
  }

  /**
   * Test addResourceWithWeight() and validateResourcesForWagedRebalance() by trying to add a resource with incomplete ResourceConfig.
   */
  @Test
  public void testAddResourceWithWeightAndValidation()
      throws IOException {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String mockInstance = "MockInstance";
    String testResourcePrefix = "TestResource";
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName, true);
    admin.addStateModelDef(clusterName, "MasterSlave", new MasterSlaveSMD());

    // Create a dummy instance
    InstanceConfig instanceConfig = new InstanceConfig(mockInstance);
    Map<String, Integer> mockInstanceCapacity =
        ImmutableMap.of("WCU", 100, "RCU", 100, "STORAGE", 100);
    instanceConfig.setInstanceCapacityMap(mockInstanceCapacity);
    admin.addInstance(clusterName, instanceConfig);
    MockParticipantManager mockParticipantManager =
        new MockParticipantManager(ZK_ADDR, clusterName, mockInstance);
    mockParticipantManager.syncStart();

    IdealState idealState = new IdealState(testResourcePrefix);
    idealState.setNumPartitions(3);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);

    ResourceConfig resourceConfig = new ResourceConfig(testResourcePrefix);
    // validate
    Map<String, Boolean> validationResult = admin.validateResourcesForWagedRebalance(clusterName,
        Collections.singletonList(testResourcePrefix));
    Assert.assertEquals(validationResult.size(), 1);
    Assert.assertFalse(validationResult.get(testResourcePrefix));
    try {
      admin.addResourceWithWeight(clusterName, idealState, resourceConfig);
      Assert.fail();
    } catch (HelixException e) {
      // OK since resourceConfig is empty
    }

    // Set PARTITION_CAPACITY_MAP
    Map<String, String> capacityDataMap =
        ImmutableMap.of("WCU", "1", "RCU", "2", "STORAGE", "3");
    resourceConfig.getRecord()
        .setMapField(ResourceConfig.ResourceConfigProperty.PARTITION_CAPACITY_MAP.name(),
            Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
                OBJECT_MAPPER.writeValueAsString(capacityDataMap)));

    // validate
    validationResult = admin.validateResourcesForWagedRebalance(clusterName,
        Collections.singletonList(testResourcePrefix));
    Assert.assertEquals(validationResult.size(), 1);
    Assert.assertFalse(validationResult.get(testResourcePrefix));

    // Add the capacity key to ClusterConfig
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    ClusterConfig clusterConfig = dataAccessor.getProperty(keyBuilder.clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("WCU", "RCU", "STORAGE"));
    dataAccessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);

    // Should succeed now
    Assert.assertTrue(admin.addResourceWithWeight(clusterName, idealState, resourceConfig));
    // validate
    validationResult = admin.validateResourcesForWagedRebalance(clusterName,
        Collections.singletonList(testResourcePrefix));
    Assert.assertEquals(validationResult.size(), 1);
    Assert.assertTrue(validationResult.get(testResourcePrefix));
  }

  /**
   * Test enabledWagedRebalance by checking the rebalancer class name changed.
   */
  @Test
  public void testEnableWagedRebalance() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String testResourcePrefix = "TestResource";
    String unaffectedResource = "UnaffectedResource";
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName, true);
    admin.addStateModelDef(clusterName, "MasterSlave", new MasterSlaveSMD());

    // Add an IdealState
    IdealState idealState = new IdealState(testResourcePrefix);
    idealState.setNumPartitions(3);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    admin.addResource(clusterName, testResourcePrefix, idealState);

    // Add an unaffected IdealState
    IdealState unaffectedIdealState = new IdealState(unaffectedResource);
    unaffectedIdealState.setNumPartitions(3);
    unaffectedIdealState.setStateModelDefRef("MasterSlave");
    unaffectedIdealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    admin.addResource(clusterName, unaffectedResource, unaffectedIdealState);

    Assert.assertTrue(admin.enableWagedRebalance(clusterName, Collections.singletonList(testResourcePrefix)));
    IdealState is = admin.getResourceIdealState(clusterName, testResourcePrefix);
    Assert.assertEquals(is.getRebalancerClassName(), WagedRebalancer.class.getName());
    is = admin.getResourceIdealState(clusterName, unaffectedResource);
    Assert.assertNotSame(is.getRebalancerClassName(), WagedRebalancer.class.getName());

    // Test non existent resource case
    try {
      admin.enableWagedRebalance(clusterName, Collections.singletonList("FakeResourceName"));
      Assert.fail("Expected HelixException");
    } catch (HelixException e) {
      Assert.assertEquals(e.getMessage(),
          "Some resources do not have IdealStates: [FakeResourceName]");
    }
  }

  @Test
  public void testAddCloudConfig() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName, true);

    CloudConfig.Builder builder = new CloudConfig.Builder();
    builder.setCloudEnabled(true);
    builder.setCloudID("TestID");
    builder.addCloudInfoSource("TestURL");
    builder.setCloudProvider(CloudProvider.CUSTOMIZED);
    builder.setCloudInfoProcessorName("TestProcessor");
    CloudConfig cloudConfig = builder.build();

    admin.addCloudConfig(clusterName, cloudConfig);

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudID(), "TestID");
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    List<String> listUrlFromZk = cloudConfigFromZk.getCloudInfoSources();
    Assert.assertEquals(listUrlFromZk.get(0), "TestURL");
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoProcessorName(), "TestProcessor");
  }


  @Test
  public void testRemoveCloudConfig() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName, true);

    CloudConfig.Builder builder = new CloudConfig.Builder();
    builder.setCloudEnabled(true);
    builder.setCloudID("TestID");
    builder.addCloudInfoSource("TestURL");
    builder.setCloudProvider(CloudProvider.CUSTOMIZED);
    builder.setCloudInfoProcessorName("TestProcessor");
    CloudConfig cloudConfig = builder.build();

    admin.addCloudConfig(clusterName, cloudConfig);

    // Read CloudConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    CloudConfig cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertTrue(cloudConfigFromZk.isCloudEnabled());
    Assert.assertEquals(cloudConfigFromZk.getCloudID(), "TestID");
    Assert.assertEquals(cloudConfigFromZk.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    List<String> listUrlFromZk = cloudConfigFromZk.getCloudInfoSources();
    Assert.assertEquals(listUrlFromZk.get(0), "TestURL");
    Assert.assertEquals(cloudConfigFromZk.getCloudInfoProcessorName(), "TestProcessor");

    // Remove Cloud Config and make sure it has been removed from Zookeeper
    admin.removeCloudConfig(clusterName);
    cloudConfigFromZk = _configAccessor.getCloudConfig(clusterName);
    Assert.assertNull(cloudConfigFromZk);
  }

  @Test
  public void testGetDomainInformation() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName, true);
    ClusterConfig clusterConfig = new ClusterConfig(clusterName);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/group/zone/rack/host");
    clusterConfig.setFaultZoneType("rack");

    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    _configAccessor.setClusterConfig(clusterName, clusterConfig);

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    for (int i = 0; i < 42; i++) {

      String hostname = "myhost" + i;
      String port = "9999";
      String instanceName = hostname + "_" + port;
      InstanceConfig instanceConfig = new InstanceConfig(instanceName);
      instanceConfig.setHostName(hostname);
      instanceConfig.setPort(port);
      if (i == 40) {
        instanceConfig.setDomain(String
            .format("invaliddomain=%s,zone=%s,rack=%s,host=%s", "mygroup" + i % 2, "myzone" + i % 4,
                "myrack" + i % 4, hostname));
      } else if (i == 41) {
        instanceConfig.setDomain("invaliddomain");
      } else {
        String domain = String
            .format("group=%s,zone=%s,rack=%s,host=%s", "mygroup" + i % 2, "myzone" + i % 4,
                "myrack" + i % 4, hostname);
        instanceConfig.setDomain(domain);
      }
      LiveInstance liveInstance = new LiveInstance(instanceName);
      liveInstance.setSessionId(UUID.randomUUID().toString());
      liveInstance.setHelixVersion(UUID.randomUUID().toString());
      accessor.setProperty(keyBuilder.liveInstance(instanceName), liveInstance);
      admin.addInstance(clusterName, instanceConfig);
      admin.enableInstance(clusterName, instanceName, true);
    }

    ClusterTopology clusterTopology = admin.getClusterTopology(clusterName);
    Assert.assertNotNull(clusterTopology);
    Map<String, List<String>> results = clusterTopology.getTopologyMap();
    Assert.assertEquals(results.size(), 2);
    Assert.assertTrue(results.containsKey("/group:mygroup0"));
    Assert.assertTrue(results.containsKey("/group:mygroup1"));
    Assert.assertEquals(results.get("/group:mygroup0").size(), 20);
    Assert.assertEquals(results.get("/group:mygroup1").size(), 20);

    results = clusterTopology.getFaultZoneMap();
    Assert.assertEquals(results.size(), 4);
    Assert.assertEquals(results.get("/group:mygroup0/zone:myzone0/rack:myrack0").size(), 10);
    Assert.assertTrue(results.get("/group:mygroup0/zone:myzone0/rack:myrack0").contains("/host"
        + ":myhost0"));

    Assert.assertEquals(clusterTopology.getInvalidInstances().size(), 2);
    Assert.assertTrue(clusterTopology.getInvalidInstances()
        .containsAll(new HashSet<>(Arrays.asList("myhost40_9999", "myhost41_9999"))));
  }

  @Test
  public void testAddCustomizedStateConfig() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);
    admin.addCluster(clusterName, true);
    CustomizedStateConfig.Builder builder =
        new CustomizedStateConfig.Builder();
    builder.addAggregationEnabledType("mockType1");
    CustomizedStateConfig customizedStateConfig = builder.build();

    admin.addCustomizedStateConfig(clusterName, customizedStateConfig);

    // Read CustomizedStateConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig configFromZk =
        _configAccessor.getCustomizedStateConfig(clusterName);
    List<String> listTypesFromZk = configFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(listTypesFromZk.get(0), "mockType1");
  }

  @Test
  public void testRemoveCustomizedStateConfig() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);
    admin.addCluster(clusterName, true);
    CustomizedStateConfig.Builder builder =
        new CustomizedStateConfig.Builder();
    builder.addAggregationEnabledType("mockType1");
    CustomizedStateConfig customizedStateConfig = builder.build();

    admin.addCustomizedStateConfig(clusterName, customizedStateConfig);

    // Read CustomizedStateConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig configFromZk =
        _configAccessor.getCustomizedStateConfig(clusterName);
    List<String> listTypesFromZk = configFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(listTypesFromZk.get(0), "mockType1");

    // Remove CustomizedStateConfig Config and make sure it has been removed from
    // Zookeeper
    admin.removeCustomizedStateConfig(clusterName);
    configFromZk = _configAccessor.getCustomizedStateConfig(clusterName);
    Assert.assertNull(configFromZk);
  }

  @Test
  public void testUpdateCustomizedStateConfig() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);
    admin.addCluster(clusterName, true);
    CustomizedStateConfig.Builder builder =
        new CustomizedStateConfig.Builder();
    builder.addAggregationEnabledType("mockType1");
    CustomizedStateConfig customizedStateConfig = builder.build();

    admin.addCustomizedStateConfig(clusterName, customizedStateConfig);

    // Read CustomizedStateConfig from Zookeeper and check the content
    ConfigAccessor _configAccessor = new ConfigAccessor(ZK_ADDR);
    CustomizedStateConfig configFromZk =
        _configAccessor.getCustomizedStateConfig(clusterName);
    List<String> listTypesFromZk = configFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(listTypesFromZk.get(0), "mockType1");

    admin.addTypeToCustomizedStateConfig(clusterName, "mockType2");
    admin.addTypeToCustomizedStateConfig(clusterName, "mockType3");
    configFromZk =
        _configAccessor.getCustomizedStateConfig(clusterName);
    listTypesFromZk = configFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(listTypesFromZk.get(0), "mockType1");
    Assert.assertEquals(listTypesFromZk.get(1), "mockType2");
    Assert.assertEquals(listTypesFromZk.get(2), "mockType3");

    admin.removeTypeFromCustomizedStateConfig(clusterName, "mockType1");
    configFromZk =
        _configAccessor.getCustomizedStateConfig(clusterName);
    listTypesFromZk = configFromZk.getAggregationEnabledTypes();
    Assert.assertEquals(listTypesFromZk.get(0), "mockType2");
    Assert.assertEquals(listTypesFromZk.get(1), "mockType3");
  }

  @Test
  public void testPurgeOfflineInstances() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    HelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.addCluster(clusterName, true);

    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();

    // set default offline duration for purge in cluster config
    ClusterConfig clusterConfig = dataAccessor.getProperty(keyBuilder.clusterConfig());
    clusterConfig.setOfflineDurationForPurge(100000L);
    dataAccessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);

    String hostname = "host1";
    String port = "9999";
    String instanceName = hostname + "_" + port;
    InstanceConfig config = new InstanceConfig(instanceName);
    config.setHostName(hostname);
    config.setPort(port);
    tool.addInstance(clusterName, config);
    tool.enableInstance(clusterName, instanceName, true);

    LiveInstance liveInstance = new LiveInstance(instanceName);
    liveInstance.setSessionId(UUID.randomUUID().toString());
    liveInstance.setHelixVersion(UUID.randomUUID().toString());
    dataAccessor.setProperty(keyBuilder.liveInstance(instanceName), liveInstance);

    dataAccessor.removeProperty(keyBuilder.liveInstance(instanceName));
    ZNRecord znRecord = new ZNRecord(instanceName);
    znRecord
        .setSimpleField("LAST_OFFLINE_TIME", String.valueOf(System.currentTimeMillis() - 50000L));
    _baseAccessor.set(PropertyPathBuilder.instanceHistory(clusterName, instanceName), znRecord, 1);

    // This purge will not remove the instance since the default offline duration is not met yet.
    tool.purgeOfflineInstances(clusterName, ClusterConfig.OFFLINE_DURATION_FOR_PURGE_NOT_SET);
    Assert.assertTrue(_gZkClient.exists(keyBuilder.instanceConfig(instanceName).getPath()),
        "Instance should still be there");

    // This purge will not remove the instance since the offline duration is not met yet.
    tool.purgeOfflineInstances(clusterName, 100000L);
    Assert.assertTrue(_gZkClient.exists(keyBuilder.instanceConfig(instanceName).getPath()),
        "Instance should still be there");

    // This purge will remove the instance as the customized offline duration is met.
    tool.purgeOfflineInstances(clusterName, 10000L);
    assertInstanceDropped(keyBuilder, instanceName);

    // Set message without config or history, mimicking race condition
    _baseAccessor.set(PropertyPathBuilder.instanceMessage(clusterName, instanceName, "testId"),
        new ZNRecord("testId"), 1);
    tool.purgeOfflineInstances(clusterName, 10000L);
    assertInstanceDropped(keyBuilder, instanceName);

    // Set config again, without instancePath or history, mimicking new instance joining
    _baseAccessor.set(PropertyPathBuilder.instanceConfig(clusterName, instanceName),
        new ZNRecord(instanceName), 1);
    tool.purgeOfflineInstances(clusterName, 10000L);
    Assert.assertTrue(_gZkClient.exists(keyBuilder.instanceConfig(instanceName).getPath()),
        "Instance should still be there");

    tool.dropCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private void assertInstanceDropped(PropertyKey.Builder keyBuilder, String instanceName) {
    Assert.assertFalse(_gZkClient.exists(keyBuilder.instanceConfig(instanceName).getPath()),
        "Instance should already be dropped");
    Assert.assertFalse(_gZkClient.exists(keyBuilder.instance(instanceName).getPath()),
        "Instance should already be dropped");
  }

  /*
   * Tests 2 APIs: enable and disable cluster pause mode.
   */
  @Test
  public void testEnableDisableClusterPauseMode() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    _gSetupTool.setupTestCluster(clusterName);
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();
    _gSetupTool.activateCluster(clusterName, controller.getClusterName(), true);

    try {
      // Should not create pause with pending cancel ST enabled because cancellation is not enabled
      try {
        ClusterManagementModeRequest request = ClusterManagementModeRequest.newBuilder()
            .withClusterName(clusterName)
            .withMode(ClusterManagementMode.Type.CLUSTER_FREEZE)
            .withCancelPendingST(true)
            .withReason(methodName)
            .build();
        _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);
        Assert.fail("Should not create pause with pending cancel ST enabled because "
            + "cancellation is not enabled");
      } catch (HelixConflictException e) {
        Assert.assertTrue(e.getMessage().startsWith("State transition cancellation not enabled"));
      }

      ClusterManagementModeRequest request = ClusterManagementModeRequest.newBuilder()
          .withClusterName(clusterName)
          .withMode(ClusterManagementMode.Type.CLUSTER_FREEZE)
          .withReason(methodName)
          .build();
      _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);
      HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
      PauseSignal pauseSignal = dataAccessor.getProperty(dataAccessor.keyBuilder().pause());

      // Verify pause signal is correctly written
      Assert.assertNotNull(pauseSignal);
      Assert.assertTrue(pauseSignal.isClusterPause());
      Assert.assertFalse(pauseSignal.getCancelPendingST());
      Assert.assertEquals(pauseSignal.getFromHost(), NetworkUtil.getLocalhostName());
      Assert.assertEquals(pauseSignal.getReason(), methodName);

      // Disable pause mode
      request = ClusterManagementModeRequest.newBuilder()
          .withClusterName(clusterName)
          .withMode(ClusterManagementMode.Type.NORMAL)
          .build();
      _gSetupTool.getClusterManagementTool().setClusterManagementMode(request);
      pauseSignal = dataAccessor.getProperty(dataAccessor.keyBuilder().pause());

      // Verify pause signal has been deleted.
      Assert.assertNull(pauseSignal);
    } finally {
      _gSetupTool.activateCluster(clusterName, controller.getClusterName(), false);
      controller.syncStop();
      _gSetupTool.deleteCluster(clusterName);
    }
  }
}
