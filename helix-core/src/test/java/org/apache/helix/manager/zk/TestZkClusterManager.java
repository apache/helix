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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.MockListener;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.store.PropertyStore;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestZkClusterManager extends ZkUnitTestBase
{
  final String className = getShortClassName();

  @Test()
  public void testController() throws Exception
  {
    System.out.println("START " + className + ".testController() at " + new Date(System.currentTimeMillis()));
    final String clusterName = CLUSTER_PREFIX + "_" + className + "_controller";

    // basic test
    if (_gZkClient.exists("/" + clusterName))
    {
      _gZkClient.deleteRecursive("/" + clusterName);
    }

    ZKHelixManager controller = new ZKHelixManager(clusterName, null,
                                                   InstanceType.CONTROLLER,
                                                   ZK_ADDR);
    try
    {
      controller.connect();
      Assert.fail("Should throw HelixException if initial cluster structure is not setup");
    } catch (HelixException e)
    {
      // OK
    }

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    controller.connect();
    AssertJUnit.assertTrue(controller.isConnected());
    controller.connect();
    AssertJUnit.assertTrue(controller.isConnected());

    MockListener listener = new MockListener();
    listener.reset();

    try
    {
      controller.addControllerListener(null);
      Assert.fail("Should throw HelixException");
    } catch (HelixException e)
    {
      // OK
    }

    controller.addControllerListener(listener);
    AssertJUnit.assertTrue(listener.isControllerChangeListenerInvoked);
    controller.removeListener(listener);

    PropertyStore<ZNRecord> store = controller.getPropertyStore();
    ZNRecord record = new ZNRecord("node_1");
    store.setProperty("node_1", record);
    record = store.getProperty("node_1");
    AssertJUnit.assertEquals("node_1", record.getId());

    controller.getMessagingService();
    controller.getHealthReportCollector();
    controller.getClusterManagmentTool();

    controller.handleNewSession();
    controller.disconnect();
    AssertJUnit.assertFalse(controller.isConnected());

    System.out.println("END " + className + ".testController() at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testAdministrator() throws Exception
  {
    System.out.println("START " + className + ".testAdministrator() at " + new Date(System.currentTimeMillis()));
    final String clusterName = CLUSTER_PREFIX + "_" + className + "_admin";

    // basic test
    if (_gZkClient.exists("/" + clusterName))
    {
      _gZkClient.deleteRecursive("/" + clusterName);
    }

    ZKHelixManager admin = new ZKHelixManager(clusterName, null,
                                              InstanceType.ADMINISTRATOR,
                                              ZK_ADDR);

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    admin.connect();
    AssertJUnit.assertTrue(admin.isConnected());

    HelixAdmin adminTool = admin.getClusterManagmentTool();
    ConfigScope scope = new ConfigScopeBuilder().forCluster(clusterName)
        .forResource("testResource").forPartition("testPartition").build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("pKey1", "pValue1");
    properties.put("pKey2", "pValue2");
    adminTool.setConfig(scope, properties);

    properties = adminTool.getConfig(scope, TestHelper.setOf("pKey1", "pKey2"));
    Assert.assertEquals(properties.size(), 2);
    Assert.assertEquals(properties.get("pKey1"), "pValue1");
    Assert.assertEquals(properties.get("pKey2"), "pValue2");

    admin.disconnect();
    AssertJUnit.assertFalse(admin.isConnected());

    System.out.println("END " + className + ".testAdministrator() at " + new Date(System.currentTimeMillis()));
  }

}
