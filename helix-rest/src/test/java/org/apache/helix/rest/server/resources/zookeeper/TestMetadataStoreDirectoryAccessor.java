package org.apache.helix.rest.server.resources.zookeeper;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.TestHelper;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.metadatastore.MetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.ZkMetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.constant.MetadataStoreRoutingConstants;
import org.apache.helix.rest.server.AbstractTestClass;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMetadataStoreDirectoryAccessor extends AbstractTestClass {
  /*
   * The following are constants to be used for testing.
   */
  private static final String TEST_NAMESPACE_URI_PREFIX = "/namespaces/" + TEST_NAMESPACE;
  private static final String NON_EXISTING_NAMESPACE_URI_PREFIX =
      "/namespaces/not-existed-namespace/metadata-store-realms/";
  private static final String TEST_REALM_1 = "testRealm1";
  private static final List<String> TEST_SHARDING_KEYS_1 =
      Arrays.asList("/sharding/key/1/a", "/sharding/key/1/b", "/sharding/key/1/c");
  private static final String TEST_REALM_2 = "testRealm2";
  private static final List<String> TEST_SHARDING_KEYS_2 =
      Arrays.asList("/sharding/key/1/d", "/sharding/key/1/e", "/sharding/key/1/f");
  private static final String TEST_REALM_3 = "testRealm3";
  private static final String TEST_SHARDING_KEY = "/sharding/key/1/x";

  // List of all ZK addresses, each of which corresponds to a namespace/routing ZK
  private List<String> _zkList;
  private MetadataStoreDirectory _metadataStoreDirectory;

  @BeforeClass
  public void beforeClass() throws Exception {
    _zkList = new ArrayList<>(ZK_SERVER_MAP.keySet());

    deleteRoutingDataPath();

    // Populate routingZkAddrMap according namespaces in helix rest server.
    // <Namespace, ZkAddr> mapping
    Map<String, String> routingZkAddrMap = ImmutableMap
        .of(HelixRestNamespace.DEFAULT_NAMESPACE_NAME, ZK_ADDR, TEST_NAMESPACE, _zkAddrTestNS);

    // Write dummy mappings in ZK
    // Create a node that represents a realm address and add 3 sharding keys to it
    ZNRecord znRecord = new ZNRecord("RoutingInfo");

    _zkList.forEach(zk -> {
      ZK_SERVER_MAP.get(zk).getZkClient().setZkSerializer(new ZNRecordSerializer());
      // Write first realm and sharding keys pair
      znRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
          TEST_SHARDING_KEYS_1);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_1,
              true);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_1,
              znRecord);

      // Create another realm and sharding keys pair
      znRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
          TEST_SHARDING_KEYS_2);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_2,
              true);
      ZK_SERVER_MAP.get(zk).getZkClient()
          .writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + TEST_REALM_2,
              znRecord);
    });

    // Create metadataStoreDirectory
    _metadataStoreDirectory = new ZkMetadataStoreDirectory(routingZkAddrMap);
  }

  @Test
  public void testGetAllMetadataStoreRealms() throws IOException {
    get(NON_EXISTING_NAMESPACE_URI_PREFIX + "metadata-store-realms", null,
        Response.Status.NOT_FOUND.getStatusCode(), false);

    String responseBody = get(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms", null,
        Response.Status.OK.getStatusCode(), true);
    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Collection<String>> queriedRealmsMap =
        OBJECT_MAPPER.readValue(responseBody, Map.class);

    Assert.assertTrue(
        queriedRealmsMap.containsKey(MetadataStoreRoutingConstants.METADATA_STORE_REALMS));

    Set<String> queriedRealmsSet =
        new HashSet<>(queriedRealmsMap.get(MetadataStoreRoutingConstants.METADATA_STORE_REALMS));
    Set<String> expectedRealms = ImmutableSet.of(TEST_REALM_1, TEST_REALM_2);

    Assert.assertEquals(queriedRealmsSet, expectedRealms);
  }

  @Test
  public void testAddMetadataStoreRealm() {
    Collection<String> previousRealms =
        _metadataStoreDirectory.getAllMetadataStoreRealms(TEST_NAMESPACE);
    Set<String> expectedRealmsSet = new HashSet<>(previousRealms);

    Assert.assertFalse(expectedRealmsSet.contains(TEST_REALM_3),
        "Metadata store directory should not have realm: " + TEST_REALM_3);

    // Test a request that has not found response.
    put(NON_EXISTING_NAMESPACE_URI_PREFIX + TEST_REALM_3, null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.NOT_FOUND.getStatusCode());

    // Successful request.
    put(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_3, null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    Collection<String> updatedRealms =
        _metadataStoreDirectory.getAllMetadataStoreRealms(TEST_NAMESPACE);
    Set<String> updateRealmsSet = new HashSet<>(updatedRealms);
    expectedRealmsSet.add(TEST_REALM_3);

    // TODO: enable asserts and add verify for refreshed MSD once write operations are ready.
//    Assert.assertEquals(updateRealmsSet, previousRealms);
  }

  @Test(dependsOnMethods = "testAddMetadataStoreRealm")
  public void testDeleteMetadataStoreRealm() {
    Collection<String> previousRealms =
        _metadataStoreDirectory.getAllMetadataStoreRealms(TEST_NAMESPACE);
    Set<String> expectedRealmsSet = new HashSet<>(previousRealms);

//    Assert.assertTrue(expectedRealmsSet.contains(TEST_REALM_3),
//        "Metadata store directory should have realm: " + TEST_REALM_3);

    // Test a request that has not found response.
    delete(NON_EXISTING_NAMESPACE_URI_PREFIX + TEST_REALM_3,
        Response.Status.NOT_FOUND.getStatusCode());

    // Successful request.
    delete(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_3,
        Response.Status.OK.getStatusCode());

    Collection<String> updatedRealms =
        _metadataStoreDirectory.getAllMetadataStoreRealms(TEST_NAMESPACE);
    Set<String> updateRealmsSet = new HashSet<>(updatedRealms);
    expectedRealmsSet.remove(TEST_REALM_3);

//    Assert.assertEquals(updateRealmsSet, previousRealms);
  }

  /*
   * Tests REST endpoints: "/sharding-keys"
   */
  @Test
  public void testGetShardingKeysInNamespace() throws IOException {
    get(NON_EXISTING_NAMESPACE_URI_PREFIX + "sharding-keys", null,
        Response.Status.NOT_FOUND.getStatusCode(), true);

    String responseBody =
        get(TEST_NAMESPACE_URI_PREFIX + "/sharding-keys", null, Response.Status.OK.getStatusCode(),
            true);
    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Collection<String>> queriedShardingKeysMap =
        OBJECT_MAPPER.readValue(responseBody, Map.class);

    Assert.assertTrue(
        queriedShardingKeysMap.containsKey(MetadataStoreRoutingConstants.SHARDING_KEYS));

    Set<String> queriedShardingKeys =
        new HashSet<>(queriedShardingKeysMap.get(MetadataStoreRoutingConstants.SHARDING_KEYS));
    Set<String> expectedShardingKeys = new HashSet<>();
    expectedShardingKeys.addAll(TEST_SHARDING_KEYS_1);
    expectedShardingKeys.addAll(TEST_SHARDING_KEYS_2);

    Assert.assertEquals(queriedShardingKeys, expectedShardingKeys);
  }

  /*
   * Tests REST endpoint: "/sharding-keys?realm={realmName}"
   */
  @Test
  public void testGetShardingKeysInRealm() throws IOException {
    // Test NOT_FOUND response for a non existed realm.
    new JerseyUriRequestBuilder(TEST_NAMESPACE_URI_PREFIX + "/sharding-keys?realm=nonExistedRealm")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).get(this);

    // Query param realm is set to empty, so NOT_FOUND response is returned.
    new JerseyUriRequestBuilder(TEST_NAMESPACE_URI_PREFIX + "/sharding-keys?realm=")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).get(this);

    // Success response.
    String responseBody = new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/sharding-keys?realm=" + TEST_REALM_1)
        .isBodyReturnExpected(true).get(this);
    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Object> queriedShardingKeysMap = OBJECT_MAPPER.readValue(responseBody, Map.class);

    // Check realm name in json response.
    Assert.assertTrue(queriedShardingKeysMap
        .containsKey(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM));
    Assert.assertEquals(
        queriedShardingKeysMap.get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM),
        TEST_REALM_1);
    Assert.assertTrue(
        queriedShardingKeysMap.containsKey(MetadataStoreRoutingConstants.SHARDING_KEYS));

    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Set<String> queriedShardingKeys = new HashSet<>((Collection<String>) queriedShardingKeysMap
        .get(MetadataStoreRoutingConstants.SHARDING_KEYS));
    Set<String> expectedShardingKeys = new HashSet<>(TEST_SHARDING_KEYS_1);

    Assert.assertEquals(queriedShardingKeys, expectedShardingKeys);
  }

  @Test
  public void testAddShardingKey() {
    Set<String> expectedShardingKeysSet = new HashSet<>(
        _metadataStoreDirectory.getAllShardingKeysInRealm(TEST_NAMESPACE, TEST_REALM_1));

    Assert.assertFalse(expectedShardingKeysSet.contains(TEST_SHARDING_KEY),
        "Realm does not have sharding key: " + TEST_SHARDING_KEY);

    // Request that gets not found response.
    put(NON_EXISTING_NAMESPACE_URI_PREFIX + TEST_REALM_1 + "/sharding-keys/" + TEST_SHARDING_KEY,
        null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.NOT_FOUND.getStatusCode());

    // Successful request.
    put(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys/"
            + TEST_SHARDING_KEY, null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    Set<String> updatedShardingKeysSet = new HashSet<>(
        _metadataStoreDirectory.getAllShardingKeysInRealm(TEST_NAMESPACE, TEST_REALM_1));
    expectedShardingKeysSet.add(TEST_SHARDING_KEY);

//    Assert.assertEquals(updatedShardingKeysSet, expectedShardingKeysSet);
  }

  @Test(dependsOnMethods = "testAddShardingKey")
  public void testDeleteShardingKey() {
    Set<String> expectedShardingKeysSet = new HashSet<>(
        _metadataStoreDirectory.getAllShardingKeysInRealm(TEST_NAMESPACE, TEST_REALM_1));

//    Assert.assertTrue(expectedShardingKeysSet.contains(TEST_SHARDING_KEY),
//        "Realm should have sharding key: " + TEST_SHARDING_KEY);

    // Request that gets not found response.
    delete(NON_EXISTING_NAMESPACE_URI_PREFIX + TEST_REALM_1 + "/sharding-keys/" + TEST_SHARDING_KEY,
        Response.Status.NOT_FOUND.getStatusCode());

    // Successful request.
    delete(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys/"
        + TEST_SHARDING_KEY, Response.Status.OK.getStatusCode());

    Set<String> updatedShardingKeysSet = new HashSet<>(
        _metadataStoreDirectory.getAllShardingKeysInRealm(TEST_NAMESPACE, TEST_REALM_1));
    expectedShardingKeysSet.remove(TEST_SHARDING_KEY);

//    Assert.assertEquals(updatedShardingKeysSet, expectedShardingKeysSet);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _metadataStoreDirectory.close();
    deleteRoutingDataPath();
  }

  private void deleteRoutingDataPath() throws Exception {
    TestHelper.verify(() -> {
      _zkList.forEach(zk -> ZK_SERVER_MAP.get(zk).getZkClient()
          .deleteRecursively(MetadataStoreRoutingConstants.ROUTING_DATA_PATH));

      for (String zk : _zkList) {
        if (ZK_SERVER_MAP.get(zk).getZkClient()
            .exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
          return false;
        }
      }

      return true;
    }, TestHelper.WAIT_DURATION);
  }
}
