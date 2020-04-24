package org.apache.helix.rest.metadatastore.accessor;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.TestHelper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.AbstractTestClass;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.http.client.methods.HttpUriRequest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZkRoutingDataWriter extends AbstractTestClass {
  private static final String DUMMY_REALM = "REALM";
  private static final String DUMMY_SHARDING_KEY = "/DUMMY/SHARDING/KEY";

  private MetadataStoreRoutingDataWriter _zkRoutingDataWriter;

  // MockWriter is used for testing request forwarding features in non-leader situations
  class MockWriter extends ZkRoutingDataWriter {
    HttpUriRequest calledRequest;

    MockWriter(String namespace, String zkAddress) {
      super(namespace, zkAddress);
    }

    // This method does not call super() because the http call should not be actually made
    @Override
    protected boolean sendRequestToLeader(HttpUriRequest request, int expectedResponseCode) {
      calledRequest = request;
      return false;
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY,
        getBaseUri().getHost());
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY,
        Integer.toString(getBaseUri().getPort()));
    _zkRoutingDataWriter = new ZkRoutingDataWriter(TEST_NAMESPACE, _zkAddrTestNS);
    clearRoutingDataPath();
  }

  @AfterClass
  public void afterClass() throws Exception {
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY);
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY);
    _zkRoutingDataWriter.close();
    clearRoutingDataPath();
  }

  @Test
  public void testAddMetadataStoreRealm() {
    _zkRoutingDataWriter.addMetadataStoreRealm(DUMMY_REALM);
    ZNRecord znRecord = _gZkClientTestNS
        .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM);
    Assert.assertNotNull(znRecord);
  }

  @Test(dependsOnMethods = "testAddMetadataStoreRealm")
  public void testDeleteMetadataStoreRealm() {
    _zkRoutingDataWriter.deleteMetadataStoreRealm(DUMMY_REALM);
    Assert.assertFalse(_gZkClientTestNS
        .exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM));
  }

  @Test(dependsOnMethods = "testDeleteMetadataStoreRealm")
  public void testAddShardingKey() {
    _zkRoutingDataWriter.addShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    ZNRecord znRecord = _gZkClientTestNS
        .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM);
    Assert.assertNotNull(znRecord);
    Assert.assertTrue(znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(DUMMY_SHARDING_KEY));
  }

  @Test(dependsOnMethods = "testAddShardingKey")
  public void testDeleteShardingKey() {
    _zkRoutingDataWriter.deleteShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    ZNRecord znRecord = _gZkClientTestNS
        .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM);
    Assert.assertNotNull(znRecord);
    Assert.assertFalse(znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(DUMMY_SHARDING_KEY));
  }

  @Test(dependsOnMethods = "testDeleteShardingKey")
  public void testSetRoutingData() {
    Map<String, List<String>> testRoutingDataMap =
        ImmutableMap.of(DUMMY_REALM, Collections.singletonList(DUMMY_SHARDING_KEY));
    _zkRoutingDataWriter.setRoutingData(testRoutingDataMap);
    ZNRecord znRecord = _gZkClientTestNS
        .readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + DUMMY_REALM);
    Assert.assertNotNull(znRecord);
    Assert.assertEquals(
        znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY).size(), 1);
    Assert.assertTrue(znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(DUMMY_SHARDING_KEY));
  }

  @Test(dependsOnMethods = "testSetRoutingData")
  public void testAddMetadataStoreRealmNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    mockWriter.addMetadataStoreRealm(DUMMY_REALM);
    Assert.assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.PUT.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, DUMMY_REALM);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  @Test(dependsOnMethods = "testAddMetadataStoreRealmNonLeader")
  public void testDeleteMetadataStoreRealmNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    mockWriter.deleteMetadataStoreRealm(DUMMY_REALM);
    Assert
        .assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.DELETE.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, DUMMY_REALM);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  @Test(dependsOnMethods = "testDeleteMetadataStoreRealmNonLeader")
  public void testAddShardingKeyNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    mockWriter.addShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    Assert.assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.PUT.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, DUMMY_REALM,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_SHARDING_KEYS_ENDPOINT, DUMMY_SHARDING_KEY);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  @Test(dependsOnMethods = "testAddShardingKeyNonLeader")
  public void testDeleteShardingKeyNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    mockWriter.deleteShardingKey(DUMMY_REALM, DUMMY_SHARDING_KEY);
    Assert
        .assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.DELETE.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, DUMMY_REALM,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_SHARDING_KEYS_ENDPOINT, DUMMY_SHARDING_KEY);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  @Test(dependsOnMethods = "testDeleteShardingKeyNonLeader")
  public void testSetRoutingDataNonLeader() {
    MockWriter mockWriter = new MockWriter(TEST_NAMESPACE, _zkAddrTestNS);
    Map<String, List<String>> testRoutingDataMap =
        ImmutableMap.of(DUMMY_REALM, Collections.singletonList(DUMMY_SHARDING_KEY));
    mockWriter.setRoutingData(testRoutingDataMap);
    Assert.assertEquals(mockWriter.calledRequest.getMethod(), HttpConstants.RestVerbs.PUT.name());
    List<String> expectedUrlParams = Arrays
        .asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, TEST_NAMESPACE,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT);
    String expectedUrl =
        getBaseUri().toString() + String.join("/", expectedUrlParams).replaceAll("//", "/")
            .substring(1);
    Assert.assertEquals(mockWriter.calledRequest.getURI().toString(), expectedUrl);
    mockWriter.close();
  }

  private void clearRoutingDataPath() throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      for (String zkRealm : _gZkClientTestNS
          .getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
        _gZkClientTestNS.delete(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm);
      }

      return _gZkClientTestNS.getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)
          .isEmpty();
    }, TestHelper.WAIT_DURATION), "Routing data path should be deleted after the tests.");
  }
}
