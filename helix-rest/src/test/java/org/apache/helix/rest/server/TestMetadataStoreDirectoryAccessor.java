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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestMetadataStoreDirectoryAccessor extends MetadataStoreDirectoryAccessorTestBase {
  /*
   * Tests REST endpoint: "GET /namespaces/{namespace}/metadata-store-namespaces"
   */
  @Test
  public void testGetAllNamespaces() throws IOException {
    String responseBody = get(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-namespaces", null,
        Response.Status.OK.getStatusCode(), true);

    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Collection<String>> queriedNamespacesMap =
        OBJECT_MAPPER.readValue(responseBody, Map.class);

    Assert.assertEquals(queriedNamespacesMap.keySet(),
        ImmutableSet.of(MetadataStoreRoutingConstants.METADATA_STORE_NAMESPACES));

    Set<String> queriedNamespacesSet = new HashSet<>(
        queriedNamespacesMap.get(MetadataStoreRoutingConstants.METADATA_STORE_NAMESPACES));
    Set<String> expectedNamespaces = ImmutableSet.of(TEST_NAMESPACE);

    Assert.assertEquals(queriedNamespacesSet, expectedNamespaces);
  }

  /*
   * Tests REST endpoint: "GET /metadata-store-realms"
   */
  @Test(dependsOnMethods = "testGetAllNamespaces")
  public void testGetAllMetadataStoreRealms() throws IOException {
    get(NON_EXISTING_NAMESPACE_URI_PREFIX + "metadata-store-realms", null,
        Response.Status.NOT_FOUND.getStatusCode(), false);

    String responseBody = get(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms", null,
        Response.Status.OK.getStatusCode(), true);
    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Collection<String>> queriedRealmsMap =
        OBJECT_MAPPER.readValue(responseBody, Map.class);

    Assert.assertEquals(queriedRealmsMap.keySet(),
        ImmutableSet.of(MetadataStoreRoutingConstants.METADATA_STORE_REALMS));

    Set<String> queriedRealmsSet =
        new HashSet<>(queriedRealmsMap.get(MetadataStoreRoutingConstants.METADATA_STORE_REALMS));
    Set<String> expectedRealms = ImmutableSet.of(TEST_REALM_1, TEST_REALM_2);

    Assert.assertEquals(queriedRealmsSet, expectedRealms);
  }

  /*
   * Tests REST endpoint: "GET /metadata-store-realms?sharding-key={sharding-key}"
   */
  @Test(dependsOnMethods = "testGetAllMetadataStoreRealms")

  public void testGetMetadataStoreRealmWithShardingKey() throws IOException {
    String shardingKey = TEST_SHARDING_KEYS_1.get(0);

    new JerseyUriRequestBuilder(
        NON_EXISTING_NAMESPACE_URI_PREFIX + "metadata-store-realms?sharding-key=" + shardingKey)
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).get(this);

    new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms?sharding-key=" + TEST_SHARDING_KEY)
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).get(this);

    new JerseyUriRequestBuilder(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms?sharding-key="
        + INVALID_TEST_SHARDING_KEY)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode()).get(this);

    String responseBody = new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms?sharding-key=" + shardingKey)
        .isBodyReturnExpected(true).get(this);

    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, String> queriedRealmMap = OBJECT_MAPPER.readValue(responseBody, Map.class);

    Map<String, String> expectedRealm = ImmutableMap
        .of(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM, TEST_REALM_1,
            MetadataStoreRoutingConstants.SINGLE_SHARDING_KEY, shardingKey);

    Assert.assertEquals(queriedRealmMap, expectedRealm);
  }

  /*
   * Tests REST endpoint: "PUT /metadata-store-realms/{realm}"
   */
  @Test(dependsOnMethods = "testGetMetadataStoreRealmWithShardingKey")
  public void testAddMetadataStoreRealm() throws InvalidRoutingDataException {
    Set<String> expectedRealmsSet = getAllRealms();
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

    // Second addition also succeeds.
    put(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_3, null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    expectedRealmsSet.add(TEST_REALM_3);
    Assert.assertEquals(getAllRealms(), expectedRealmsSet);
  }

  /*
   * Tests REST endpoint: "DELETE /metadata-store-realms/{realm}"
   */
  @Test(dependsOnMethods = "testAddMetadataStoreRealm")
  public void testDeleteMetadataStoreRealm() throws InvalidRoutingDataException {
    Set<String> expectedRealmsSet = getAllRealms();
    // Test a request that has not found response.
    delete(NON_EXISTING_NAMESPACE_URI_PREFIX + TEST_REALM_3,
        Response.Status.NOT_FOUND.getStatusCode());

    // Successful request.
    delete(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_3,
        Response.Status.OK.getStatusCode());

    // Second deletion also succeeds.
    delete(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_3,
        Response.Status.OK.getStatusCode());

    Set<String> updateRealmsSet = getAllRealms();
    expectedRealmsSet.remove(TEST_REALM_3);
    Assert.assertEquals(updateRealmsSet, expectedRealmsSet);
  }

  /*
   * Tests REST endpoint: "GET /sharding-keys"
   */
  @Test(dependsOnMethods = "testDeleteMetadataStoreRealm")
  public void testGetShardingKeysInNamespace() throws IOException {
    get(NON_EXISTING_NAMESPACE_URI_PREFIX + "sharding-keys", null,
        Response.Status.NOT_FOUND.getStatusCode(), true);

    String responseBody =
        get(TEST_NAMESPACE_URI_PREFIX + "/sharding-keys", null, Response.Status.OK.getStatusCode(),
            true);
    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Object> queriedShardingKeysMap = OBJECT_MAPPER.readValue(responseBody, Map.class);

    Assert.assertEquals(queriedShardingKeysMap.keySet(), ImmutableSet
        .of(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_NAMESPACE,
            MetadataStoreRoutingConstants.SHARDING_KEYS));

    Assert.assertEquals(
        queriedShardingKeysMap.get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_NAMESPACE),
        TEST_NAMESPACE);

    @SuppressWarnings("unchecked")
    Set<String> queriedShardingKeys = new HashSet<>((Collection<String>) queriedShardingKeysMap
        .get(MetadataStoreRoutingConstants.SHARDING_KEYS));
    Set<String> expectedShardingKeys = new HashSet<>();
    expectedShardingKeys.addAll(TEST_SHARDING_KEYS_1);
    expectedShardingKeys.addAll(TEST_SHARDING_KEYS_2);

    Assert.assertEquals(queriedShardingKeys, expectedShardingKeys);
  }

  /*
   * Tests REST endpoint: "GET /sharding-keys?prefix={prefix}"
   */
  @SuppressWarnings("unchecked")
  @Test(dependsOnMethods = "testGetShardingKeysInNamespace")
  public void testGetShardingKeysUnderPath() throws IOException {
    new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/sharding-keys?prefix=" + INVALID_TEST_SHARDING_KEY)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode()).get(this);

    // Test non existed prefix and empty sharding keys in response.
    String responseBody = new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/sharding-keys?prefix=/non/Existed/Prefix")
        .isBodyReturnExpected(true).get(this);

    Map<String, Object> queriedShardingKeysMap = OBJECT_MAPPER.readValue(responseBody, Map.class);
    Collection<Map<String, String>> emptyKeysList =
        (Collection<Map<String, String>>) queriedShardingKeysMap
            .get(MetadataStoreRoutingConstants.SHARDING_KEYS);
    Assert.assertTrue(emptyKeysList.isEmpty());

    // Success response with non empty sharding keys.
    String shardingKeyPrefix = "/sharding/key";
    responseBody = new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/sharding-keys?prefix=" + shardingKeyPrefix)
        .isBodyReturnExpected(true).get(this);

    queriedShardingKeysMap = OBJECT_MAPPER.readValue(responseBody, Map.class);

    // Check fields.
    Assert.assertEquals(queriedShardingKeysMap.keySet(), ImmutableSet
        .of(MetadataStoreRoutingConstants.SHARDING_KEY_PATH_PREFIX,
            MetadataStoreRoutingConstants.SHARDING_KEYS));

    // Check sharding key prefix in json response.
    Assert.assertEquals(
        queriedShardingKeysMap.get(MetadataStoreRoutingConstants.SHARDING_KEY_PATH_PREFIX),
        shardingKeyPrefix);

    Collection<Map<String, String>> queriedShardingKeys =
        (Collection<Map<String, String>>) queriedShardingKeysMap
            .get(MetadataStoreRoutingConstants.SHARDING_KEYS);
    Set<Map<String, String>> queriedShardingKeysSet = new HashSet<>(queriedShardingKeys);
    Set<Map<String, String>> expectedShardingKeysSet = new HashSet<>();

    TEST_SHARDING_KEYS_1.forEach(key -> expectedShardingKeysSet.add(ImmutableMap
        .of(MetadataStoreRoutingConstants.SINGLE_SHARDING_KEY, key,
            MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM, TEST_REALM_1)));

    TEST_SHARDING_KEYS_2.forEach(key -> expectedShardingKeysSet.add(ImmutableMap
        .of(MetadataStoreRoutingConstants.SINGLE_SHARDING_KEY, key,
            MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM, TEST_REALM_2)));

    Assert.assertEquals(queriedShardingKeysSet, expectedShardingKeysSet);
  }

  /*
   * Tests REST endpoint: "GET /routing-data"
   */
  @Test(dependsOnMethods = "testGetShardingKeysUnderPath")
  public void testGetRoutingData() throws IOException {
    /*
     * responseBody:
     * {
     *   "namespace" : "test-namespace",
     *   "routingData" : [ {
     *     "realm" : "testRealm2",
     *     "shardingKeys" : [ "/sharding/key/1/d", "/sharding/key/1/e", "/sharding/key/1/f" ]
     *   }, {
     *     "realm" : "testRealm1",
     *     "shardingKeys" : [ "/sharding/key/1/a", "/sharding/key/1/b", "/sharding/key/1/c" ]
     *   } ]
     * }
     */
    String responseBody = new JerseyUriRequestBuilder(TEST_NAMESPACE_URI_PREFIX + "/routing-data")
        .isBodyReturnExpected(true).get(this);

    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Object> queriedShardingKeysMap = OBJECT_MAPPER.readValue(responseBody, Map.class);

    // Check fields.
    Assert.assertEquals(queriedShardingKeysMap.keySet(), ImmutableSet
        .of(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_NAMESPACE,
            MetadataStoreRoutingConstants.ROUTING_DATA));

    // Check namespace in json response.
    Assert.assertEquals(
        queriedShardingKeysMap.get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_NAMESPACE),
        TEST_NAMESPACE);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> queriedShardingKeys =
        (List<Map<String, Object>>) queriedShardingKeysMap
            .get(MetadataStoreRoutingConstants.ROUTING_DATA);

    Set<Map<String, Object>> queriedShardingKeysSet = new HashSet<>(queriedShardingKeys);
    Set<Map<String, Object>> expectedShardingKeysSet = ImmutableSet.of(ImmutableMap
        .of(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM, TEST_REALM_1,
            MetadataStoreRoutingConstants.SHARDING_KEYS, TEST_SHARDING_KEYS_1), ImmutableMap
        .of(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM, TEST_REALM_2,
            MetadataStoreRoutingConstants.SHARDING_KEYS, TEST_SHARDING_KEYS_2));

    Assert.assertEquals(queriedShardingKeysSet, expectedShardingKeysSet);
  }

  /*
   * Tests REST endpoint: "GET /metadata-store-realms/{realm}/sharding-keys"
   */
  @Test(dependsOnMethods = "testGetRoutingData")
  public void testGetShardingKeysInRealm() throws IOException {
    // Test NOT_FOUND response for a non existed realm.
    new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/nonExistedRealm/sharding-keys")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).get(this);

    // Success response for "GET /metadata-store-realms/{realm}/sharding-keys"
    String responseBody = new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys")
        .isBodyReturnExpected(true).get(this);

    verifyRealmShardingKeys(responseBody);
  }

  /*
   * Tests REST endpoint: "GET /metadata-store-realms/{realm}/sharding-keys?prefix={prefix}"
   */
  @SuppressWarnings("unchecked")
  @Test(dependsOnMethods = "testGetShardingKeysInRealm")
  public void testGetRealmShardingKeysUnderPath() throws IOException {
    new JerseyUriRequestBuilder(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1
        + "/sharding-keys?prefix=" + INVALID_TEST_SHARDING_KEY)
        .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode()).get(this);

    // Test non existed prefix and empty sharding keys in response.
    String responseBody = new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1
            + "/sharding-keys?prefix=/non/Existed/Prefix").isBodyReturnExpected(true).get(this);

    Map<String, Object> queriedShardingKeysMap = OBJECT_MAPPER.readValue(responseBody, Map.class);
    Collection<Map<String, String>> emptyKeysList =
        (Collection<Map<String, String>>) queriedShardingKeysMap
            .get(MetadataStoreRoutingConstants.SHARDING_KEYS);
    Assert.assertTrue(emptyKeysList.isEmpty());

    // Test non existed realm and empty sharding keys in response.
    responseBody = new JerseyUriRequestBuilder(TEST_NAMESPACE_URI_PREFIX
        + "/metadata-store-realms/nonExistedRealm/sharding-keys?prefix=/sharding/key")
        .isBodyReturnExpected(true).get(this);

    queriedShardingKeysMap = OBJECT_MAPPER.readValue(responseBody, Map.class);
    emptyKeysList = (Collection<Map<String, String>>) queriedShardingKeysMap
        .get(MetadataStoreRoutingConstants.SHARDING_KEYS);
    Assert.assertTrue(emptyKeysList.isEmpty());

    // Valid query params and non empty sharding keys.
    String shardingKeyPrefix = "/sharding/key/1";
    responseBody = new JerseyUriRequestBuilder(
        TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1
            + "/sharding-keys?prefix=" + shardingKeyPrefix).isBodyReturnExpected(true).get(this);

    queriedShardingKeysMap = OBJECT_MAPPER.readValue(responseBody, Map.class);

    // Check fields.
    Assert.assertEquals(queriedShardingKeysMap.keySet(), ImmutableSet
        .of(MetadataStoreRoutingConstants.SHARDING_KEY_PATH_PREFIX,
            MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM,
            MetadataStoreRoutingConstants.SHARDING_KEYS));

    // Check sharding key prefix in json response.
    Assert.assertEquals(
        queriedShardingKeysMap.get(MetadataStoreRoutingConstants.SHARDING_KEY_PATH_PREFIX),
        shardingKeyPrefix);

    Assert.assertEquals(
        queriedShardingKeysMap.get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM),
        TEST_REALM_1);

    Set<String> queriedShardingKeys = new HashSet<>((Collection<String>) queriedShardingKeysMap
        .get(MetadataStoreRoutingConstants.SHARDING_KEYS));
    Set<String> expectedShardingKeys = new HashSet<>(TEST_SHARDING_KEYS_1);

    Assert.assertEquals(queriedShardingKeys, expectedShardingKeys);
  }

  /*
   * Tests REST endpoint: "PUT /metadata-store-realms/{realm}/sharding-keys/{sharding-key}"
   */
  @Test(dependsOnMethods = "testGetRealmShardingKeysUnderPath")
  public void testAddShardingKey() throws InvalidRoutingDataException {
    Set<String> expectedShardingKeysSet = getAllShardingKeysInTestRealm1();
    Assert.assertFalse(expectedShardingKeysSet.contains(TEST_SHARDING_KEY),
        "Realm does not have sharding key: " + TEST_SHARDING_KEY);

    // Request that gets not found response.
    put(NON_EXISTING_NAMESPACE_URI_PREFIX + TEST_REALM_1 + "/sharding-keys" + TEST_SHARDING_KEY,
        null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.NOT_FOUND.getStatusCode());

    put(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys"
            + "//" + INVALID_TEST_SHARDING_KEY, null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());

    // Successful request.
    put(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys"
            + TEST_SHARDING_KEY, null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // Idempotency
    put(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys"
            + TEST_SHARDING_KEY, null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // Invalid
    put(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_2 + "/sharding-keys"
            + TEST_SHARDING_KEY, null, Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());

    expectedShardingKeysSet.add(TEST_SHARDING_KEY);
    Assert.assertEquals(getAllShardingKeysInTestRealm1(), expectedShardingKeysSet);
  }

  /*
   * Tests REST endpoint: "PUT /metadata-store-realms/{realm}/sharding-keys/{sharding-key}"
   */
  @Test(dependsOnMethods = "testAddShardingKey")
  public void testDeleteShardingKey() throws InvalidRoutingDataException {
    Set<String> expectedShardingKeysSet = getAllShardingKeysInTestRealm1();

    // Request that gets not found response.
    delete(NON_EXISTING_NAMESPACE_URI_PREFIX + TEST_REALM_1 + "/sharding-keys" + TEST_SHARDING_KEY,
        Response.Status.NOT_FOUND.getStatusCode());

    // Successful request.
    delete(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys"
        + TEST_SHARDING_KEY, Response.Status.OK.getStatusCode());

    // Idempotency
    delete(TEST_NAMESPACE_URI_PREFIX + "/metadata-store-realms/" + TEST_REALM_1 + "/sharding-keys"
        + TEST_SHARDING_KEY, Response.Status.OK.getStatusCode());

    expectedShardingKeysSet.remove(TEST_SHARDING_KEY);
    Assert.assertEquals(getAllShardingKeysInTestRealm1(), expectedShardingKeysSet);
  }

  @Test(dependsOnMethods = "testDeleteShardingKey")
  public void testSetRoutingData() throws InvalidRoutingDataException, IOException {
    Map<String, List<String>> routingData = new HashMap<>();
    routingData.put(TEST_REALM_1, TEST_SHARDING_KEYS_2);
    routingData.put(TEST_REALM_2, TEST_SHARDING_KEYS_1);
    String routingDataString = OBJECT_MAPPER.writeValueAsString(routingData);

    Map<String, String> badFormatRoutingData = new HashMap<>();
    badFormatRoutingData.put(TEST_REALM_1, TEST_REALM_2);
    badFormatRoutingData.put(TEST_REALM_2, TEST_REALM_1);
    String badFormatRoutingDataString = OBJECT_MAPPER.writeValueAsString(badFormatRoutingData);

    // Request that gets not found response.
    put("/namespaces/non-existing-namespace"
            + MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT, null,
        Entity.entity(routingDataString, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.NOT_FOUND.getStatusCode());

    put(TEST_NAMESPACE_URI_PREFIX
            + MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT, null,
        Entity.entity("?", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());

    put(TEST_NAMESPACE_URI_PREFIX
            + MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT, null,
        Entity.entity(badFormatRoutingDataString, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.BAD_REQUEST.getStatusCode());

    // Successful request.
    put(TEST_NAMESPACE_URI_PREFIX
            + MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT, null,
        Entity.entity(routingDataString, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    Assert.assertEquals(getRawRoutingData(), routingData);
  }

  private void verifyRealmShardingKeys(String responseBody) throws IOException {
    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Object> queriedShardingKeysMap = OBJECT_MAPPER.readValue(responseBody, Map.class);

    // Check fields in JSON response.
    Assert.assertEquals(queriedShardingKeysMap.keySet(), ImmutableSet
        .of(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM,
            MetadataStoreRoutingConstants.SHARDING_KEYS));

    // Check realm name in json response.
    Assert.assertEquals(
        queriedShardingKeysMap.get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM),
        TEST_REALM_1);

    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Set<String> queriedShardingKeys = new HashSet<>((Collection<String>) queriedShardingKeysMap
        .get(MetadataStoreRoutingConstants.SHARDING_KEYS));
    Set<String> expectedShardingKeys = new HashSet<>(TEST_SHARDING_KEYS_1);

    Assert.assertEquals(queriedShardingKeys, expectedShardingKeys);
  }
}
