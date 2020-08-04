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
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;

import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZooKeeperAccessor extends AbstractTestClass {
  private ZkBaseDataAccessor<byte[]> _testBaseDataAccessor;

  @BeforeClass
  public void beforeClass() {
    _testBaseDataAccessor = new ZkBaseDataAccessor<>(ZK_ADDR, new ZkSerializer() {
      @Override
      public byte[] serialize(Object o)
          throws ZkMarshallingError {
        return (byte[]) o;
      }

      @Override
      public Object deserialize(byte[] bytes)
          throws ZkMarshallingError {
        return new String(bytes);
      }
    }, ZkBaseDataAccessor.ZkClientType.DEDICATED);
  }

  @AfterClass
  public void afterClass() {
    _testBaseDataAccessor.close();
  }

  @Test
  public void testExists()
      throws IOException {
    String path = "/path";
    Assert.assertFalse(_testBaseDataAccessor.exists(path, AccessOption.PERSISTENT));
    Map<String, Boolean> result;
    String data = new JerseyUriRequestBuilder("zookeeper{}?command=exists").format(path)
        .isBodyReturnExpected(true).get(this);
    result = OBJECT_MAPPER.readValue(data, HashMap.class);
    Assert.assertTrue(result.containsKey("exists"));
    Assert.assertFalse(result.get("exists"));

    // Create a ZNode and check again
    String content = "testExists";
    Assert.assertTrue(
        _testBaseDataAccessor.create(path, content.getBytes(), AccessOption.PERSISTENT));
    Assert.assertTrue(_testBaseDataAccessor.exists(path, AccessOption.PERSISTENT));

    data = new JerseyUriRequestBuilder("zookeeper{}?command=exists").format(path)
        .isBodyReturnExpected(true).get(this);
    result = OBJECT_MAPPER.readValue(data, HashMap.class);
    Assert.assertTrue(result.containsKey("exists"));
    Assert.assertTrue(result.get("exists"));

    // Clean up
    _testBaseDataAccessor.remove(path, AccessOption.PERSISTENT);
  }

  @Test
  public void testGetData()
      throws IOException {
    String path = "/path";
    String content = "testGetData";

    Assert.assertFalse(_testBaseDataAccessor.exists(path, AccessOption.PERSISTENT));
    // Expect BAD_REQUEST
    String data = new JerseyUriRequestBuilder("zookeeper{}?command=getStringData").format(path)
        .isBodyReturnExpected(false)
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).get(this);

    // Now write data and test
    _testBaseDataAccessor.create(path, content.getBytes(), AccessOption.PERSISTENT);
    // Get the stat object
    Stat expectedStat = _testBaseDataAccessor.getStat(path, AccessOption.PERSISTENT);
    String getStatKey = "getStat";

    // Test getStringData
    String getStringDataKey = "getStringData";
    data = new JerseyUriRequestBuilder("zookeeper{}?command=getStringData").format(path)
        .isBodyReturnExpected(true).get(this);
    Map<String, Object> stringResult = OBJECT_MAPPER.readValue(data, Map.class);
    Assert.assertTrue(stringResult.containsKey(getStringDataKey));
    Assert.assertEquals(stringResult.get(getStringDataKey), content);
    Assert.assertTrue(stringResult.containsKey(getStatKey));
    Assert.assertEquals(stringResult.get(getStatKey), ZKUtil.fromStatToMap(expectedStat));

    // Test getBinaryData
    String getBinaryDataKey = "getBinaryData";
    data = new JerseyUriRequestBuilder("zookeeper{}?command=getBinaryData").format(path)
        .isBodyReturnExpected(true).get(this);
    Map<String, Object> binaryResult = OBJECT_MAPPER.readValue(data, Map.class);
    Assert.assertTrue(binaryResult.containsKey(getBinaryDataKey));
    // Note: The response's byte array is encoded into a String using Base64 (for safety),
    // so the user must decode with Base64 to get the original byte array back
    byte[] decodedBytes = Base64.getDecoder().decode((String) binaryResult.get(getBinaryDataKey));
    Assert.assertEquals(decodedBytes, content.getBytes());
    Assert.assertTrue(binaryResult.containsKey(getStatKey));
    Assert.assertEquals(binaryResult.get(getStatKey), ZKUtil.fromStatToMap(expectedStat));

    // Clean up
    _testBaseDataAccessor.remove(path, AccessOption.PERSISTENT);
  }

  @Test
  public void testGetChildren()
      throws IOException {
    String path = "/path";
    String childrenKey = "/children";
    int numChildren = 20;

    // Create a ZNode and its children
    for (int i = 0; i < numChildren; i++) {
      _testBaseDataAccessor.create(path + childrenKey, null, AccessOption.PERSISTENT_SEQUENTIAL);
    }

    // Verify
    String getChildrenKey = "getChildren";
    String data = new JerseyUriRequestBuilder("zookeeper{}?command=getChildren").format(path)
        .isBodyReturnExpected(true).get(this);
    Map<String, List<String>> result = OBJECT_MAPPER.readValue(data, HashMap.class);
    Assert.assertTrue(result.containsKey(getChildrenKey));
    Assert.assertEquals(result.get(getChildrenKey).size(), numChildren);

    // Check that all children are indeed created with PERSISTENT_SEQUENTIAL
    result.get(getChildrenKey).forEach(child -> {
      Assert.assertTrue(child.contains("children"));
    });

    // Clean up
    _testBaseDataAccessor.remove(path, AccessOption.PERSISTENT);
  }

  @Test
  public void testGetStat() throws IOException {
    String path = "/path/getStat";

    // Make sure it returns a NOT FOUND if there is no ZNode
    String data = new JerseyUriRequestBuilder("zookeeper{}?command=getStat").format(path)
        .isBodyReturnExpected(false)
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).get(this);

    // Create a test ZNode (ephemeral)
    _testBaseDataAccessor.create(path, null, AccessOption.PERSISTENT);
    Stat stat = _testBaseDataAccessor.getStat(path, AccessOption.PERSISTENT);
    Map<String, String> expectedFields = ZKUtil.fromStatToMap(stat);
    expectedFields.put("path", path);

    // Verify with the REST endpoint
    data = new JerseyUriRequestBuilder("zookeeper{}?command=getStat").format(path)
        .isBodyReturnExpected(true).get(this);
    Map<String, String> result = OBJECT_MAPPER.readValue(data, HashMap.class);

    Assert.assertEquals(result, expectedFields);

    // Clean up
    _testBaseDataAccessor.remove(path, AccessOption.PERSISTENT);
  }

  @Test
  public void testDelete() {
    String path = "/path";
    String deletePath = path + "/delete";

    try {
      // 1. Create a persistent node. Delete shall fail.
      _testBaseDataAccessor.create(deletePath, null, AccessOption.PERSISTENT);
      new JerseyUriRequestBuilder("zookeeper{}").format(deletePath)
          .expectedReturnStatusCode(Response.Status.FORBIDDEN.getStatusCode()).delete(this);
      Assert.assertTrue(_testBaseDataAccessor.exists(deletePath, AccessOption.PERSISTENT));
      // 2. Try to delete a non-exist ZNode
      new JerseyUriRequestBuilder("zookeeper{}").format(deletePath + "/foobar")
          .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).delete(this);
      // 3. Create an ephemeral node. Delete shall be done successfully.
      _testBaseDataAccessor.remove(deletePath, AccessOption.PERSISTENT);
      _testBaseDataAccessor.create(deletePath, null, AccessOption.EPHEMERAL);
      // Verify with the REST endpoint
      new JerseyUriRequestBuilder("zookeeper{}").format(deletePath)
          .expectedReturnStatusCode(Response.Status.OK.getStatusCode()).delete(this);
      Assert.assertFalse(_testBaseDataAccessor.exists(deletePath, AccessOption.PERSISTENT));
    } finally {
      // Clean up
      _testBaseDataAccessor.remove(path, AccessOption.PERSISTENT);
    }
  }
}
