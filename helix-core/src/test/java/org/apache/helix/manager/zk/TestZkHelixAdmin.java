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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestZkHelixAdmin extends ZkUnitTestBase {
  @Test()
  public void testZkHelixAdmin() {
    System.out.println("START testZkHelixAdmin at " + new Date(System.currentTimeMillis()));

    final String clusterName = getShortClassName();
    String rootPath = "/" + clusterName;
    if (_gZkClient.exists(rootPath)) {
      _gZkClient.deleteRecursive(rootPath);
    }

    HelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient));
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient));

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

    InstanceConfig config = new InstanceConfig("host1_9999");
    config.setHostName("host1");
    config.setPort("9999");
    tool.addInstance(clusterName, config);
    tool.enableInstance(clusterName, "host1_9999", true);
    String path = PropertyPathBuilder.instance(clusterName, "host1_9999");
    AssertJUnit.assertTrue(_gZkClient.exists(path));

    try {
      tool.addInstance(clusterName, config);
      Assert.fail("should fail if add an alredy-existing instance");
    } catch (HelixException e) {
      // OK
    }
    config = tool.getInstanceConfig(clusterName, "host1_9999");
    AssertJUnit.assertEquals(config.getId(), "host1_9999");

    tool.dropInstance(clusterName, config);
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
    } catch (HelixException e) {
      // OK
    } catch (IllegalArgumentException ex) {
      // OK
    }

    tool.addStateModelDef(clusterName, "MasterSlave", new StateModelDefinition(
        StateModelConfigGenerator.generateConfigForMasterSlave()));
    stateModelRecord = StateModelConfigGenerator.generateConfigForMasterSlave();
    tool.addStateModelDef(clusterName, stateModelRecord.getId(), new StateModelDefinition(
        stateModelRecord));
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

    // test config support
    // ConfigScope scope = new ConfigScopeBuilder().forCluster(clusterName)
    // .forResource("testResource").forPartition("testPartition").build();
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTITION).forCluster(clusterName)
            .forResource("testResource").forPartition("testPartition").build();

    Map<String, String> properties = new HashMap<String, String>();
    properties.put("pKey1", "pValue1");
    properties.put("pKey2", "pValue2");

    // make sure calling set/getConfig() many times will not drain zkClient resources
    // int nbOfZkClients = ZkClient.getNumberOfConnections();
    for (int i = 0; i < 100; i++) {
      tool.setConfig(scope, properties);
      Map<String, String> newProperties =
          tool.getConfig(scope, new ArrayList<String>(properties.keySet()));
      Assert.assertEquals(newProperties.size(), 2);
      Assert.assertEquals(newProperties.get("pKey1"), "pValue1");
      Assert.assertEquals(newProperties.get("pKey2"), "pValue2");
    }
    // Assert.assertTrue(ZkClient.getNumberOfConnections() - nbOfZkClients < 5);

    System.out.println("END testZkHelixAdmin at " + new Date(System.currentTimeMillis()));
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

    tool.addStateModelDef(clusterName, "MasterSlave", new StateModelDefinition(
        StateModelConfigGenerator.generateConfigForMasterSlave()));
    tool.addResource(clusterName, "test-db", 4, "MasterSlave");
    Map<String, String> resourceConfig = new HashMap<String, String>();
    resourceConfig.put("key1", "value1");
    tool.setConfig(new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE)
        .forCluster(clusterName).forResource("test-db").build(), resourceConfig);

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
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
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
    admin.addStateModelDef(clusterName, "MasterSlave", new StateModelDefinition(
        StateModelConfigGenerator.generateConfigForMasterSlave()));
    admin.addResource(clusterName, resourceName, 4, "MasterSlave");
    admin.enableResource(clusterName, resourceName, false);
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    Assert.assertFalse(idealState.isEnabled());
    admin.enableResource(clusterName, resourceName, true);
    idealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    Assert.assertTrue(idealState.isEnabled());
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testGetResourcesWithTag() {
    String TEST_TAG = "TestTAG";

    final String clusterName = getShortClassName();
    String rootPath = "/" + clusterName;
    if (_gZkClient.exists(rootPath)) {
      _gZkClient.deleteRecursive(rootPath);
    }

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
        Arrays.asList(new String[]{"1", "2"}));
    admin.enablePartition(false, clusterName, instanceName, testResourcePrefix + "1",
        Arrays.asList(new String[]{"2", "3", "4"}));
    InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "0").size(), 2);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "1").size(), 3);

    // Test enable partition across resources
    instanceConfig.setInstanceEnabledForPartition("2", true);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "0").size(), 1);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "1").size(), 2);

    // Test disable partition across resources
    instanceConfig.setInstanceEnabledForPartition("10", false);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "0").size(), 2);
    Assert.assertEquals(instanceConfig.getDisabledPartitions(testResourcePrefix + "1").size(), 3);
  }

  @Test
  public void testLegacyEnableDisablePartition() {
    String instanceName = "TestInstanceLegacy";
    String testResourcePrefix = "TestResourceLegacy";
    ZNRecord record = new ZNRecord(instanceName);
    List<String> disabledPartitions =
        new ArrayList<String>(Arrays.asList(new String[] { "1", "2", "3" }));
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
}
