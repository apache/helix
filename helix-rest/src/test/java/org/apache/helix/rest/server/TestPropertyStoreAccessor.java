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
import javax.ws.rs.core.Response;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestPropertyStoreAccessor extends AbstractTestClass {
  private static final String TEST_CLUSTER = "TestCluster_0";
  private static final String ZNRECORD_PATH =
      PropertyPathBuilder.propertyStore(TEST_CLUSTER) + "/ZnRecord";
  private static final ZNRecord TEST_ZNRECORD = new ZNRecord("TestContent");
  private static final String CUSTOM_PATH =
      PropertyPathBuilder.propertyStore(TEST_CLUSTER) + "/NonZnRecord";
  private static final String TEST_CONTENT = "TestContent";

  private ZkBaseDataAccessor<String> _customDataAccessor;

  @BeforeClass
  public void init() {
    _customDataAccessor = new ZkBaseDataAccessor<>(ZK_ADDR, new ZkSerializer() {
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
    // initially prepare the datas in different paths
    Assert
        .assertTrue(_customDataAccessor.create(CUSTOM_PATH, TEST_CONTENT, AccessOption.PERSISTENT));
    Assert.assertTrue(_baseAccessor.create(ZNRECORD_PATH, TEST_ZNRECORD, AccessOption.PERSISTENT));
  }

  @AfterClass
  public void close() {
    if (_customDataAccessor != null) {
      _customDataAccessor.close();
    }
  }

  @Test
  public void testGetPropertyStoreWithZNRecordData()
      throws IOException {
    String data =
        new JerseyUriRequestBuilder("clusters/{}/propertyStore/ZnRecord").format(TEST_CLUSTER)
            .isBodyReturnExpected(true).get(this);
    ZNRecord record = OBJECT_MAPPER.reader(ZNRecord.class).readValue(data);
    Assert.assertEquals(record.getId(), TEST_ZNRECORD.getId());
  }

  @Test
  public void testGetPropertyStoreWithTestStringData()
      throws IOException {
    String data =
        new JerseyUriRequestBuilder("clusters/{}/propertyStore/NonZnRecord").format(TEST_CLUSTER)
            .isBodyReturnExpected(true).get(this);

    ZNRecord record = OBJECT_MAPPER.reader(ZNRecord.class).readValue(data);
    Assert.assertEquals(record.getSimpleField("default"), TEST_CONTENT);
  }

  @Test
  public void testGetPropertyStoreWithEmptyDataPath() {
    Response response =
        new JerseyUriRequestBuilder("clusters/{}/propertyStore/EmptyPath").format(TEST_CLUSTER)
            .isBodyReturnExpected(true).getResponse(this);
    Assert.assertEquals(response.getStatus(), HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void testGetPropertyStoreWithInValidPath() {
    String path = "/context/";
    Response response =
        new JerseyUriRequestBuilder("clusters/{}/propertyStore" + path).format(TEST_CLUSTER)
            .getResponse(this);
    Assert.assertEquals(response.getStatus(), HttpStatus.SC_BAD_REQUEST);
  }
}
