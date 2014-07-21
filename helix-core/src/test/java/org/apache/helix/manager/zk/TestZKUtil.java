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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZKUtil extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestZKUtil.class);

  String clusterName = "TestZKUtil";
  ZkClient _zkClient;

  @BeforeClass()
  public void beforeClass() throws IOException, Exception {
    _zkClient = new ZkClient(_zkaddr);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    if (_zkClient.exists("/" + clusterName)) {
      _zkClient.deleteRecursive("/" + clusterName);
    }

    boolean result = ZKUtil.isClusterSetup(clusterName, _zkClient);
    AssertJUnit.assertFalse(result);
    result = ZKUtil.isClusterSetup(null, _zkClient);
    AssertJUnit.assertFalse(result);

    result = ZKUtil.isClusterSetup(null, null);
    AssertJUnit.assertFalse(result);

    result = ZKUtil.isClusterSetup(clusterName, null);
    AssertJUnit.assertFalse(result);

    TestHelper.setupEmptyCluster(_zkClient, clusterName);
  }

  @AfterClass()
  public void afterClass() {
    _zkClient.close();
  }

  @Test()
  public void testIsClusterSetup() {
    boolean result = ZKUtil.isClusterSetup(clusterName, _zkClient);
    AssertJUnit.assertTrue(result);
  }

  @Test()
  public void testChildrenOperations() {
    List<ZNRecord> list = new ArrayList<ZNRecord>();
    list.add(new ZNRecord("id1"));
    list.add(new ZNRecord("id2"));
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString());
    ZKUtil.createChildren(_zkClient, path, list);
    list = ZKUtil.getChildren(_zkClient, path);
    AssertJUnit.assertEquals(2, list.size());

    ZKUtil.dropChildren(_zkClient, path, list);
    ZKUtil.dropChildren(_zkClient, path, new ZNRecord("id1"));
    list = ZKUtil.getChildren(_zkClient, path);
    AssertJUnit.assertEquals(0, list.size());

    ZKUtil.dropChildren(_zkClient, path, (List<ZNRecord>) null);
  }

  @Test()
  public void testUpdateIfExists() {
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString(), "id3");
    ZNRecord record = new ZNRecord("id4");
    ZKUtil.updateIfExists(_zkClient, path, record, false);
    AssertJUnit.assertFalse(_zkClient.exists(path));
    _zkClient.createPersistent(path);
    ZKUtil.updateIfExists(_zkClient, path, record, false);
    AssertJUnit.assertTrue(_zkClient.exists(path));
    record = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals("id4", record.getId());
  }

  @Test()
  public void testSubtract() {
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString(), "id5");
    ZNRecord record = new ZNRecord("id5");
    record.setSimpleField("key1", "value1");
    _zkClient.createPersistent(path, record);
    ZKUtil.subtract(_zkClient, path, record);
    record = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertNull(record.getSimpleField("key1"));
  }

  @Test()
  public void testNullChildren() {
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString(), "id6");
    ZKUtil.createChildren(_zkClient, path, (List<ZNRecord>) null);
  }

  @Test()
  public void testCreateOrUpdate() {
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString(), "id7");
    ZNRecord record = new ZNRecord("id7");
    ZKUtil.createOrUpdate(_zkClient, path, record, true, true);
    record = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals("id7", record.getId());
  }

  @Test()
  public void testCreateOrReplace() {
    String path =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString(), "id8");
    ZNRecord record = new ZNRecord("id8");
    ZKUtil.createOrReplace(_zkClient, path, record, true);
    record = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals("id8", record.getId());
    record = new ZNRecord("id9");
    ZKUtil.createOrReplace(_zkClient, path, record, true);
    record = _zkClient.<ZNRecord> readData(path);
    AssertJUnit.assertEquals("id9", record.getId());
  }
}
