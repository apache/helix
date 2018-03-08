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

import org.apache.helix.AccessOption;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.MockListener;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestZkClusterManager extends ZkUnitTestBase {
  final String className = getShortClassName();

  @Test()
  public void testController() throws Exception {
    System.out.println("START " + className + ".testController() at "
        + new Date(System.currentTimeMillis()));
    final String clusterName = CLUSTER_PREFIX + "_" + className + "_controller";

    // basic test
    if (_gZkClient.exists("/" + clusterName)) {
      _gZkClient.deleteRecursively("/" + clusterName);
    }

    ZKHelixManager controller =
        new ZKHelixManager(clusterName, null, InstanceType.CONTROLLER, ZK_ADDR);

    try {
      controller.connect();
      Assert.fail("Should throw HelixException if initial cluster structure is not setup");
    } catch (HelixException e) {
      // OK
    }

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    controller.connect();
    AssertJUnit.assertTrue(controller.isConnected());
    controller.connect();
    AssertJUnit.assertTrue(controller.isConnected());

    MockListener listener = new MockListener();
    listener.reset();

    try {
      controller.addControllerListener(null);
      Assert.fail("Should throw HelixException");
    } catch (HelixException e) {
      // OK
    }

    Builder keyBuilder = new Builder(controller.getClusterName());
    controller.addControllerListener(listener);
    AssertJUnit.assertTrue(listener.isControllerChangeListenerInvoked);
    controller.removeListener(keyBuilder.controller(), listener);

    ZkHelixPropertyStore<ZNRecord> store = controller.getHelixPropertyStore();
    ZNRecord record = new ZNRecord("node_1");
    int options = 0;
    store.set("/node_1", record, AccessOption.PERSISTENT);
    Stat stat = new Stat();
    record = store.get("/node_1", stat, options);
    AssertJUnit.assertEquals("node_1", record.getId());

    controller.getMessagingService();
    controller.getClusterManagmentTool();

    controller.handleNewSession();
    controller.disconnect();
    AssertJUnit.assertFalse(controller.isConnected());

    System.out.println("END " + className + ".testController() at "
        + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testLiveInstanceInfoProvider() throws Exception {
    System.out.println("START " + className + ".testLiveInstanceInfoProvider() at "
        + new Date(System.currentTimeMillis()));
    final String clusterName = CLUSTER_PREFIX + "_" + className + "_liveInstanceInfoProvider";
    class provider implements LiveInstanceInfoProvider {
      boolean _flag = false;

      public provider(boolean genSessionId) {
        _flag = genSessionId;
      }

      @Override
      public ZNRecord getAdditionalLiveInstanceInfo() {
        ZNRecord record = new ZNRecord("info");
        record.setSimpleField("simple", "value");
        List<String> listFieldVal = new ArrayList<String>();
        listFieldVal.add("val1");
        listFieldVal.add("val2");
        listFieldVal.add("val3");
        record.setListField("list", listFieldVal);
        Map<String, String> mapFieldVal = new HashMap<String, String>();
        mapFieldVal.put("k1", "val1");
        mapFieldVal.put("k2", "val2");
        mapFieldVal.put("k3", "val3");
        record.setMapField("map", mapFieldVal);
        if (_flag) {
          record.setSimpleField("SESSION_ID", "value");
          record.setSimpleField("LIVE_INSTANCE", "value");
          record.setSimpleField("Others", "value");
        }
        return record;
      }
    }

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
    int[] ids = {
        0, 1, 2, 3, 4, 5
    };
    setupInstances(clusterName, ids);

    // ///////////////////
    ZKHelixManager manager =
        new ZKHelixManager(clusterName, "localhost_0", InstanceType.PARTICIPANT, ZK_ADDR);
    manager.connect();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    LiveInstance liveInstance =
        accessor.getProperty(accessor.keyBuilder().liveInstance("localhost_0"));
    Assert.assertTrue(liveInstance.getRecord().getListFields().size() == 0);
    Assert.assertTrue(liveInstance.getRecord().getMapFields().size() == 0);
    Assert.assertTrue(liveInstance.getRecord().getSimpleFields().size() == 3);

    manager = new ZKHelixManager(clusterName, "localhost_1", InstanceType.PARTICIPANT, ZK_ADDR);
    manager.setLiveInstanceInfoProvider(new provider(false));

    manager.connect();
    accessor = manager.getHelixDataAccessor();

    liveInstance = accessor.getProperty(accessor.keyBuilder().liveInstance("localhost_1"));
    Assert.assertTrue(liveInstance.getRecord().getListFields().size() == 1);
    Assert.assertTrue(liveInstance.getRecord().getMapFields().size() == 1);
    Assert.assertTrue(liveInstance.getRecord().getSimpleFields().size() == 4);

    manager = new ZKHelixManager(clusterName, "localhost_2", InstanceType.PARTICIPANT, ZK_ADDR);
    manager.setLiveInstanceInfoProvider(new provider(true));

    manager.connect();
    accessor = manager.getHelixDataAccessor();

    liveInstance = accessor.getProperty(accessor.keyBuilder().liveInstance("localhost_2"));
    Assert.assertTrue(liveInstance.getRecord().getListFields().size() == 1);
    Assert.assertTrue(liveInstance.getRecord().getMapFields().size() == 1);
    Assert.assertTrue(liveInstance.getRecord().getSimpleFields().size() == 5);
    Assert.assertFalse(liveInstance.getSessionId().equals("value"));
    Assert.assertFalse(liveInstance.getLiveInstance().equals("value"));

    // //////////////////////////////////

    MockParticipantManager manager2 =
        new MockParticipantManager(ZK_ADDR, clusterName, "localhost_3");

    manager2.setLiveInstanceInfoProvider(new provider(true));

    manager2.connect();
    accessor = manager2.getHelixDataAccessor();

    liveInstance = accessor.getProperty(accessor.keyBuilder().liveInstance("localhost_3"));
    Assert.assertTrue(liveInstance.getRecord().getListFields().size() == 1);
    Assert.assertTrue(liveInstance.getRecord().getMapFields().size() == 1);
    Assert.assertTrue(liveInstance.getRecord().getSimpleFields().size() == 5);
    Assert.assertFalse(liveInstance.getSessionId().equals("value"));
    Assert.assertFalse(liveInstance.getLiveInstance().equals("value"));
    String sessionId = liveInstance.getSessionId();

    ZkTestHelper.expireSession(manager2.getZkClient());
    Thread.sleep(1000);

    liveInstance = accessor.getProperty(accessor.keyBuilder().liveInstance("localhost_3"));
    Assert.assertTrue(liveInstance.getRecord().getListFields().size() == 1);
    Assert.assertTrue(liveInstance.getRecord().getMapFields().size() == 1);
    Assert.assertTrue(liveInstance.getRecord().getSimpleFields().size() == 5);
    Assert.assertFalse(liveInstance.getSessionId().equals("value"));
    Assert.assertFalse(liveInstance.getLiveInstance().equals("value"));
    Assert.assertFalse(sessionId.equals(liveInstance.getSessionId()));

    System.out.println("END " + className + ".testLiveInstanceInfoProvider() at "
        + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testAdministrator() throws Exception {
    System.out.println("START " + className + ".testAdministrator() at "
        + new Date(System.currentTimeMillis()));
    final String clusterName = CLUSTER_PREFIX + "_" + className + "_admin";

    // basic test
    if (_gZkClient.exists("/" + clusterName)) {
      _gZkClient.deleteRecursively("/" + clusterName);
    }

    ZKHelixManager admin =
        new ZKHelixManager(clusterName, null, InstanceType.ADMINISTRATOR, ZK_ADDR);

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    admin.connect();
    AssertJUnit.assertTrue(admin.isConnected());

    HelixAdmin adminTool = admin.getClusterManagmentTool();
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTITION).forCluster(clusterName)
            .forResource("testResource").forPartition("testPartition").build();

    Map<String, String> properties = new HashMap<String, String>();
    properties.put("pKey1", "pValue1");
    properties.put("pKey2", "pValue2");
    adminTool.setConfig(scope, properties);

    properties = adminTool.getConfig(scope, Arrays.asList("pKey1", "pKey2"));
    Assert.assertEquals(properties.size(), 2);
    Assert.assertEquals(properties.get("pKey1"), "pValue1");
    Assert.assertEquals(properties.get("pKey2"), "pValue2");

    admin.disconnect();
    AssertJUnit.assertFalse(admin.isConnected());

    System.out.println("END " + className + ".testAdministrator() at "
        + new Date(System.currentTimeMillis()));
  }
}
