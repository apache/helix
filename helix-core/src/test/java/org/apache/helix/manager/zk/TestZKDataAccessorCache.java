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
import java.util.Date;
import java.util.List;

import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.ConfigScope.ConfigScopeProperty;
import org.apache.helix.manager.zk.ZKDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZKDataAccessorCache extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestZKDataAccessorCache.class);
  private ZKDataAccessor _accessor;
  private String _clusterName;
  private ZkClient _zkClient;

  @BeforeClass
  public void beforeClass() throws IOException, Exception
  {
    _clusterName = CLUSTER_PREFIX + "_" + getShortClassName();

    System.out.println("START TestZKCacheDataAccessor at " + new Date(System.currentTimeMillis()));
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());

    if (_zkClient.exists("/" + _clusterName))
    {
      _zkClient.deleteRecursive("/" + _clusterName);
    }
    _zkClient.createPersistent(
        PropertyPathConfig.getPath(PropertyType.CONFIGS, _clusterName,
            ConfigScopeProperty.CLUSTER.toString(), _clusterName), true);
    _zkClient.createPersistent(
        PropertyPathConfig.getPath(PropertyType.CONFIGS, _clusterName,
        ConfigScopeProperty.PARTICIPANT.toString()), true);
    _zkClient.createPersistent(
        PropertyPathConfig.getPath(PropertyType.CONFIGS, _clusterName,
            ConfigScopeProperty.RESOURCE.toString()), true);
    _zkClient.createPersistent(PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName),
        true);
    _zkClient.createPersistent(PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, _clusterName),
        true);
    _zkClient.createPersistent(
        PropertyPathConfig.getPath(PropertyType.LIVEINSTANCES, _clusterName), true);
    _zkClient.createPersistent(
        PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS, _clusterName), true);
    _zkClient.createPersistent(PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, _clusterName,
        "localhost_12918", "123456"), true);

    _accessor = new ZKDataAccessor(_clusterName, _zkClient);
  }

  @AfterClass
  public void afterClass()
  {
    _zkClient.close();
    System.out.println("END TestZKCacheDataAccessor at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testAccessorCache()
  {
    testAccessorCache(PropertyType.IDEALSTATES);
    testAccessorCache(PropertyType.STATEMODELDEFS);
    testAccessorCache(PropertyType.LIVEINSTANCES);
    testAccessorCache(PropertyType.CONFIGS, ConfigScopeProperty.PARTICIPANT.toString());
    testAccessorCache(PropertyType.EXTERNALVIEW);
    testAccessorCache(PropertyType.CURRENTSTATES, "localhost_12918", "123456");
  }

  private void testAccessorCache(PropertyType type, String... keys)
  {
    String parentPath = PropertyPathConfig.getPath(type, _clusterName, keys);
    _zkClient.createPersistent(parentPath + "/child1", new ZNRecord("child1"));
    ZNRecord record2 = new ZNRecord("child2");
    _zkClient.createPersistent(parentPath + "/child2", record2);

    List<ZNRecord> records = _accessor.getChildValues(type, keys);
    LOG.debug("records:" + records);
    Assert.assertNotNull(getRecord(records, "child1"));
    Assert.assertNotNull(getRecord(records, "child2"));

    // no data change
    List<ZNRecord> newRecords = _accessor.getChildValues(type, keys);
    LOG.debug("new records:" + newRecords);
    Assert.assertEquals(getRecord(newRecords, "child1"), getRecord(records, "child1"));

    // change value of an existing znode
    record2.setSimpleField("key1", "value1");
    _zkClient.writeData(parentPath + "/child2", record2);
    newRecords = _accessor.getChildValues(type, keys);
    LOG.debug("new records:" + newRecords);
    Assert.assertEquals(getRecord(newRecords, "child2").getSimpleField("key1"), "value1");
    Assert.assertNotSame(getRecord(newRecords, "child2"), getRecord(records, "child2"));

    // add a new child
    _zkClient.createPersistent(parentPath + "/child3", new ZNRecord("child3"));
    records = newRecords;
    newRecords = _accessor.getChildValues(type, keys);
    LOG.debug("new records:" + newRecords);
    Assert.assertNull(getRecord(records, "child3"));
    Assert.assertNotNull(getRecord(newRecords, "child3"));

    // delete a child
    _zkClient.delete(parentPath + "/child2");
    records = newRecords;
    newRecords = _accessor.getChildValues(type, keys);
    LOG.debug("new records:" + newRecords);
    Assert.assertNotNull(getRecord(records, "child2"));
    Assert.assertNull(getRecord(newRecords, "child2"),
        "Should be null, since child2 has been deleted");
  }

  private ZNRecord getRecord(List<ZNRecord> list, String id)
  {
    for (ZNRecord record : list)
    {
      if (record.getId().equals(id))
      {
        return record;
      }
    }
    return null;
  }
}
