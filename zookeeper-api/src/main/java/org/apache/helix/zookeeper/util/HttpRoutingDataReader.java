package org.apache.helix.zookeeper.util;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.datamodel.TrieRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;


public class HttpRoutingDataReader {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static volatile Map<String, List<String>> _rawRoutingData;
  private static volatile MetadataStoreRoutingData _metadataStoreRoutingData;

  /**
   * This class is a Singleton.
   */
  private HttpRoutingDataReader() {
  }

  /**
   * Fetches routing data from the data source via HTTP.
   * @return a mapping from "metadata store realm addresses" to lists of "metadata store sharding keys", where the sharding keys in a value list all route to the realm address in the key disallows a meaningful mapping to be returned
   */
  public static Map<String, List<String>> getRawRoutingData()
      throws IOException {
    if (_rawRoutingData == null) {
      synchronized (HttpRoutingDataReader.class) {
        if (_rawRoutingData == null) {
          Map<String, List<String>> rawRoutingData = new HashMap<>();
          String msdsEndpoint =
              System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
          if (msdsEndpoint == null || msdsEndpoint.isEmpty()) {
            throw new IllegalStateException(
                "HttpRoutingDataReader was unable to find a valid MSDS endpoint String in System Properties!");
          }

          CloseableHttpClient httpClient = HttpClients.createDefault();
          // TODO: Currently we are making multiple HTTP calls. We should cut it down to one call to retrieve all raw routing data
          // TODO: See https://github.com/apache/helix/issues/798
          // Get all realms
          // For each realm, get all sharding keys
          // Put <realm, sharding keys> in rawRoutingData
          HttpGet requestAllRealmNames = new HttpGet(
              msdsEndpoint + MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT);
          CloseableHttpResponse response = httpClient.execute(requestAllRealmNames);
          HttpEntity entity = response.getEntity();
          if (entity != null) {
            String resultStr = EntityUtils.toString(entity);
            Map<String, Collection<String>> resultMap =
                OBJECT_MAPPER.readValue(resultStr, Map.class);
            Collection<String> realmNames =
                resultMap.get(MetadataStoreRoutingConstants.METADATA_STORE_REALMS);
            if (realmNames != null && !realmNames.isEmpty()) {
              for (String realmName : realmNames) {
                HttpGet requestAllShardingKeysForRealm = new HttpGet(
                    msdsEndpoint + MetadataStoreRoutingConstants.MSDS_GET_ALL_SHARDING_KEYS_ENDPOINT
                        + "?realm=" + realmName);
                CloseableHttpResponse shardingKeyResponse =
                    httpClient.execute(requestAllShardingKeysForRealm);
                HttpEntity shardingKeyEntity = shardingKeyResponse.getEntity();
                if (shardingKeyEntity != null) {
                  String shardingKeyResultStr = EntityUtils.toString(shardingKeyEntity);
                  Map<String, Collection<String>> shardingKeyMap =
                      OBJECT_MAPPER.readValue(shardingKeyResultStr, Map.class);
                  Collection<String> shardingKeys =
                      shardingKeyMap.get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM);
                  if (shardingKeys != null && !shardingKeys.isEmpty()) {
                    rawRoutingData.put(realmName, new ArrayList<>(shardingKeys));
                  }
                }
              }
            }
          }
          // Update the reference
          _rawRoutingData = rawRoutingData;
        }
      }
    }
    return _rawRoutingData;
  }

  public MetadataStoreRoutingData getMetadataStoreRoutingData()
      throws IOException, InvalidRoutingDataException {
    if (_metadataStoreRoutingData == null) {
      synchronized (HttpRoutingDataReader.class) {
        if (_metadataStoreRoutingData == null) {
          _metadataStoreRoutingData = new TrieRoutingData(getRawRoutingData());
        }
      }
    }
    return _metadataStoreRoutingData;
  }
}
