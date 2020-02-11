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
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableSet;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.rest.metadatastore.constant.MetadataStoreDirectoryConstants;
import org.apache.helix.rest.metadatastore.constant.MetadataStoreRoutingConstants;
import org.apache.helix.rest.server.AbstractTestClass;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMetadataStoreDirectoryAccessor extends AbstractTestClass {
  /*
   * The following are constants to be used for testing.
   */
  private static final String TEST_REALM_1 = "testRealm1";
  private static final List<String> TEST_SHARDING_KEYS_1 =
      Arrays.asList("/sharding/key/1/a", "/sharding/key/1/b", "/sharding/key/1/c");
  private static final String TEST_REALM_2 = "testRealm2";
  private static final List<String> TEST_SHARDING_KEYS_2 =
      Arrays.asList("/sharding/key/1/d", "/sharding/key/1/e", "/sharding/key/1/f");

  // List of all ZK addresses, each of which corresponds to a namespace/routing ZK
  private List<String> _zkList;

  @BeforeClass
  public void beforeClass() {
    _zkList = new ArrayList<>(ZK_SERVER_MAP.keySet());

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
  }

  @Test
  public void testGetAllMetadataStoreRealms() throws IOException {
    String responseBody =
        get("metadata-store-realms", null, Response.Status.OK.getStatusCode(), true);
    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Collection<String>> queriedRealmsMap =
        OBJECT_MAPPER.readValue(responseBody, Map.class);

    Assert.assertTrue(
        queriedRealmsMap.containsKey(MetadataStoreDirectoryConstants.METADATA_STORE_REALMS_NAME));

    Set<String> queriedRealmsSet = new HashSet<>(
        queriedRealmsMap.get(MetadataStoreDirectoryConstants.METADATA_STORE_REALMS_NAME));
    Set<String> expectedRealms = ImmutableSet.of(TEST_REALM_1, TEST_REALM_2);

    Assert.assertEquals(queriedRealmsSet, expectedRealms);
  }

  /*
   * Tests REST endpoints: "/sharding-keys"
   */
  @Test
  public void testGetShardingKeysInNamespace() throws IOException {
    String responseBody = get("/sharding-keys", null, Response.Status.OK.getStatusCode(), true);
    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Collection<String>> queriedShardingKeysMap =
        OBJECT_MAPPER.readValue(responseBody, Map.class);

    Assert.assertTrue(
        queriedShardingKeysMap.containsKey(MetadataStoreDirectoryConstants.SHARDING_KEYS_NAME));

    Set<String> queriedShardingKeys = new HashSet<>(
        queriedShardingKeysMap.get(MetadataStoreDirectoryConstants.SHARDING_KEYS_NAME));
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
    new JerseyUriRequestBuilder("/sharding-keys?realm=nonExistedRealm")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).get(this);

    // Query param realm is set to empty, so NOT_FOUND response is returned.
    new JerseyUriRequestBuilder("/sharding-keys?realm=")
        .expectedReturnStatusCode(Response.Status.NOT_FOUND.getStatusCode()).get(this);

    // Success response.
    String responseBody = new JerseyUriRequestBuilder("/sharding-keys?realm=" + TEST_REALM_1)
        .isBodyReturnExpected(true).get(this);
    // It is safe to cast the object and suppress warnings.
    @SuppressWarnings("unchecked")
    Map<String, Collection<String>> queriedShardingKeysMap =
        OBJECT_MAPPER.readValue(responseBody, Map.class);

    Assert.assertTrue(
        queriedShardingKeysMap.containsKey(MetadataStoreDirectoryConstants.SHARDING_KEYS_NAME));

    Set<String> queriedShardingKeys = new HashSet<>(
        queriedShardingKeysMap.get(MetadataStoreDirectoryConstants.SHARDING_KEYS_NAME));
    Set<String> expectedShardingKeys = new HashSet<>(TEST_SHARDING_KEYS_1);

    Assert.assertEquals(queriedShardingKeys, expectedShardingKeys);
  }

  @AfterClass
  public void afterClass() {
    _zkList.forEach(zk -> ZK_SERVER_MAP.get(zk).getZkClient()
        .deleteRecursive(MetadataStoreRoutingConstants.ROUTING_DATA_PATH));
  }
}
