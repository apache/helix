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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.jcabi.aspects.RetryOnFailure;
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
   * @return a mapping from "metadata store realm addresses" to lists of
   * "metadata store sharding keys", where the sharding keys in a value list all route to
   * the realm address in the key disallows a meaningful mapping to be returned
   */
  @RetryOnFailure(attempts = 3, delay = 1, unit = TimeUnit.SECONDS)
  public static Map<String, List<String>> getRawRoutingData()
      throws IOException {
    if (_rawRoutingData == null) {
      synchronized (HttpRoutingDataReader.class) {
        if (_rawRoutingData == null) {
          String msdsEndpoint =
              System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
          if (msdsEndpoint == null || msdsEndpoint.isEmpty()) {
            throw new IllegalStateException(
                "HttpRoutingDataReader was unable to find a valid MSDS endpoint String in System Properties!");
          }

          // Note: HttpClient's timeout settings are system timeout settings by default
          CloseableHttpClient httpClient = HttpClients.createDefault();
          // Update the reference
          _rawRoutingData = getAllRoutingData(httpClient, msdsEndpoint);
          // Close any open resources
          httpClient.close();
        }
      }
    }
    return _rawRoutingData;
  }

  /**
   * Returns the routing data read from MSDS in a MetadataStoreRoutingData format.
   * @return
   * @throws IOException
   * @throws InvalidRoutingDataException
   */
  public static MetadataStoreRoutingData getMetadataStoreRoutingData()
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

  /**
   * Makes an HTTP call to fetch all routing data.
   * @param httpClient
   * @param msdsEndpoint
   * @return
   * @throws IOException
   */
  private static Map<String, List<String>> getAllRoutingData(CloseableHttpClient httpClient,
      String msdsEndpoint)
      throws IOException {
    HttpGet requestAllData =
        new HttpGet(msdsEndpoint + "/" + MetadataStoreRoutingConstants.ROUTING_DATA);
    CloseableHttpResponse response = httpClient.execute(requestAllData);
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      String resultStr = EntityUtils.toString(entity);
      @SuppressWarnings("unchecked")
      Map<String, Object> resultMap = OBJECT_MAPPER.readValue(resultStr, Map.class);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> routingDataList =
          (List<Map<String, Object>>) resultMap.get(MetadataStoreRoutingConstants.ROUTING_DATA);
      @SuppressWarnings("unchecked")
      Map<String, List<String>> routingData = routingDataList.stream().collect(Collectors.toMap(
          mapEntry -> (String) mapEntry
              .get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM),
          mapEntry -> (List<String>) mapEntry.get(MetadataStoreRoutingConstants.SHARDING_KEYS)));
      return routingData;
    }
    return Collections.emptyMap();
  }
}

