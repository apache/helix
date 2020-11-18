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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZKUtil extends ZkUnitTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestZKUtil.class);

  String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();

  @BeforeClass()
  public void beforeClass() throws Exception {
    boolean result = ZKUtil.isClusterSetup(clusterName, _gZkClient);
    AssertJUnit.assertFalse(result);
    result = ZKUtil.isClusterSetup(null, _gZkClient);
    AssertJUnit.assertFalse(result);

    result = ZKUtil.isClusterSetup(null, (HelixZkClient) null);
    AssertJUnit.assertFalse(result);

    result = ZKUtil.isClusterSetup(clusterName, (HelixZkClient) null);
    AssertJUnit.assertFalse(result);

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
  }

  @AfterClass()
  public void afterClass() {
    deleteCluster(clusterName);
  }

  @AfterMethod()
  public void afterMethod() {
    String path = PropertyPathBuilder.instanceConfig(clusterName);
    _gZkClient.deleteRecursively(path);
    _gZkClient.createPersistent(path);
  }

  @Test()
  public void testIsClusterSetup() {
    boolean result = ZKUtil.isClusterSetup(clusterName, _gZkClient);
    AssertJUnit.assertTrue(result);
  }

  @Test()
  public void testChildrenOperations() {
    List<ZNRecord> list = new ArrayList<ZNRecord>();
    list.add(new ZNRecord("id1"));
    list.add(new ZNRecord("id2"));
    String path = PropertyPathBuilder.instanceConfig(clusterName);
    ZKUtil.createChildren(_gZkClient, path, list);
    list = ZKUtil.getChildren(_gZkClient, path);
    AssertJUnit.assertEquals(2, list.size());

    ZKUtil.dropChildren(_gZkClient, path, list);
    ZKUtil.dropChildren(_gZkClient, path, new ZNRecord("id1"));
    list = ZKUtil.getChildren(_gZkClient, path);
    AssertJUnit.assertEquals(0, list.size());

    ZKUtil.dropChildren(_gZkClient, path, (List<ZNRecord>) null);
  }

  @Test()
  public void testUpdateIfExists() {
    String path = PropertyPathBuilder.instanceConfig(clusterName, "id3");
    ZNRecord record = new ZNRecord("id4");
    ZKUtil.updateIfExists(_gZkClient, path, record, false);
    AssertJUnit.assertFalse(_gZkClient.exists(path));
    _gZkClient.createPersistent(path);
    ZKUtil.updateIfExists(_gZkClient, path, record, false);
    AssertJUnit.assertTrue(_gZkClient.exists(path));
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals("id4", record.getId());
  }

  @Test()
  public void testSubtract() {
    String path = PropertyPathBuilder.instanceConfig(clusterName, "id5");
    ZNRecord record = new ZNRecord("id5");
    record.setSimpleField("key1", "value1");
    _gZkClient.createPersistent(path, record);
    ZKUtil.subtract(_gZkClient, path, record);
    record = _gZkClient.readData(path);
    AssertJUnit.assertNull(record.getSimpleField("key1"));
  }

  @Test()
  public void testNullChildren() {
    String path = PropertyPathBuilder.instanceConfig(clusterName, "id6");
    ZKUtil.createChildren(_gZkClient, path, (List<ZNRecord>) null);
  }

  @Test()
  public void testCreateOrMerge() {
    String path = PropertyPathBuilder.instanceConfig(clusterName, "id7");
    ZNRecord record = new ZNRecord("id7");
    List<String> list = Arrays.asList("value1");
    record.setListField("list", list);
    ZKUtil.createOrMerge(_gZkClient, path, record, true, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals(list, record.getListField("list"));

    record = new ZNRecord("id7");
    List<String> list2 = Arrays.asList("value2");
    record.setListField("list", list2);
    ZKUtil.createOrMerge(_gZkClient, path, record, true, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals(Arrays.asList("value1", "value2"), record.getListField("list"));

    Map<String, String> map = new HashMap<String, String>() {{
      put("k1", "v1");
    }};
    record.setMapField("map", map);
    ZKUtil.createOrMerge(_gZkClient, path, record, true, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals(map, record.getMapField("map"));

    record = new ZNRecord("id7");
    Map<String, String> map2 = new HashMap<String, String>() {{
      put("k2", "v2");
    }};
    record.setMapField("map", map2);
    ZKUtil.createOrMerge(_gZkClient, path, record, true, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals(new HashMap<String, String>() {{
      put("k1", "v1");
      put("k2", "v2");
    }}, record.getMapField("map"));
  }

  @Test()
  public void testCreateOrReplace() {
    String path = PropertyPathBuilder.instanceConfig(clusterName, "id8");
    ZNRecord record = new ZNRecord("id8");
    ZKUtil.createOrReplace(_gZkClient, path, record, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals("id8", record.getId());
    record = new ZNRecord("id9");
    ZKUtil.createOrReplace(_gZkClient, path, record, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals("id9", record.getId());
  }

  @Test()
  public void testCreateOrUpdate() {
    String path = PropertyPathBuilder.instanceConfig(clusterName, "id9");
    ZNRecord record = new ZNRecord("id9");
    ZKUtil.createOrMerge(_gZkClient, path, record, true, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals("id9", record.getId());

    record = new ZNRecord("id9");
    List<String> list = Arrays.asList("value1", "value2");
    record.setListField("list", list);
    ZKUtil.createOrUpdate(_gZkClient, path, record, true, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals(list, record.getListField("list"));

    record = new ZNRecord("id9");
    List<String> list2 = Arrays.asList("value3", "value4");
    record.setListField("list", list2);
    ZKUtil.createOrUpdate(_gZkClient, path, record, true, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals(list2, record.getListField("list"));

    Map<String, String> map = new HashMap<String, String>() {{
      put("k1", "v1");
    }};
    record.setMapField("map", map);
    ZKUtil.createOrUpdate(_gZkClient, path, record, true, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals(map, record.getMapField("map"));

    record = new ZNRecord("id9");
    Map<String, String> map2 = new HashMap<String, String>() {{
      put("k2", "v2");
    }};
    record.setMapField("map", map2);
    ZKUtil.createOrUpdate(_gZkClient, path, record, true, true);
    record = _gZkClient.readData(path);
    AssertJUnit.assertEquals(new HashMap<String, String>() {{
      put("k2", "v2");
    }}, record.getMapField("map"));
  }
}
