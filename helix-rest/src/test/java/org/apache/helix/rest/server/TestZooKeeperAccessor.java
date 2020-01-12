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

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
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
        return o.toString().getBytes();
      }

      @Override
      public Object deserialize(byte[] bytes)
          throws ZkMarshallingError {
        return new String(bytes);
      }
    });
  }

  @AfterClass
  public void afterClass() {
    _testBaseDataAccessor.close();
  }

  @Test
  public void testExists() {
    String path = "/path";
    Assert.assertFalse(_testBaseDataAccessor.exists(path, AccessOption.PERSISTENT));

    // Create a ZNode and check again
    String content = "testExists";
    Assert.assertTrue(_testBaseDataAccessor.create(path, content.getBytes(), AccessOption.PERSISTENT));
    Assert.assertTrue(_testBaseDataAccessor.exists(path, AccessOption.PERSISTENT));

    String data = new JerseyUriRequestBuilder("zookeeper{}?command=exists").format(path)
        .isBodyReturnExpected(true).get(this);

    // Clean up
    _testBaseDataAccessor.remove(path, AccessOption.PERSISTENT);
  }

  @Test
  public void testGetData() {
    String path = "path";
    String content = "testGetData";

    // First, write data
    _testBaseDataAccessor.create(path, content.getBytes(), AccessOption.PERSISTENT);

    String data = new JerseyUriRequestBuilder("zookeeper/getData{}").format(path)
        .isBodyReturnExpected(true).get(this);
  }

  @Test
  public void testGetChildren() {

  }
}
