package org.apache.helix.zookeeper.routing;

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
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.zookeeper.exception.MultiZkException;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultBackoffStrategy;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


/**
 * HTTP-based RoutingDataReader that makes an HTTP call to Metadata Store Directory Service (REST)
 * to fetch routing data.
 */
public class HttpRoutingDataReader implements RoutingDataReader {
  private static final int DEFAULT_HTTP_TIMEOUT_IN_MS = 5000;

  /**
   * Returns a map form of metadata store routing data.
   * The map fields stand for metadata store realm address (key), and a corresponding list of ZK
   * path sharding keys (key).
   * @param endpoint
   * @return
   */
  @Override
  public Map<String, List<String>> getRawRoutingData(String endpoint) {
    try {
      String routingDataJson = getAllRoutingData(endpoint);
      return parseRoutingData(routingDataJson);
    } catch (IOException e) {
      throw new MultiZkException(e);
    }
  }

  /**
   * Makes an HTTP call to fetch all routing data.
   * @return
   * @throws IOException
   */
  private String getAllRoutingData(String msdsEndpoint) throws IOException {
    // Note that MSDS_ENDPOINT should provide high-availability - it risks becoming a single point
    // of failure if it's backed by a single IP address/host
    // Retry count is 3 by default.
    HttpGet requestAllData = new HttpGet(
        msdsEndpoint + MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT);

    // Define timeout configs
    RequestConfig config = RequestConfig.custom().setConnectTimeout(DEFAULT_HTTP_TIMEOUT_IN_MS)
        .setConnectionRequestTimeout(DEFAULT_HTTP_TIMEOUT_IN_MS)
        .setSocketTimeout(DEFAULT_HTTP_TIMEOUT_IN_MS).build();

    try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config)
        .setConnectionBackoffStrategy(new DefaultBackoffStrategy())
        .setRetryHandler(new DefaultHttpRequestRetryHandler()).build()) {
      // Return the JSON because try-resources clause closes the CloseableHttpResponse
      HttpEntity entity = httpClient.execute(requestAllData).getEntity();
      if (entity == null) {
        throw new IOException("Response's entity is null!");
      }
      return EntityUtils.toString(entity, "UTF-8");
    }
  }

  /**
   * Returns the raw routing data in a Map< ZkRealm, List of shardingKeys > format.
   * @param routingDataJson
   * @return
   */
  private Map<String, List<String>> parseRoutingData(String routingDataJson) throws IOException {
    if (routingDataJson != null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> resultMap = new ObjectMapper().readValue(routingDataJson, Map.class);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> routingDataList =
          (List<Map<String, Object>>) resultMap.get(MetadataStoreRoutingConstants.ROUTING_DATA);
      @SuppressWarnings("unchecked")
      Map<String, List<String>> routingData = routingDataList.stream().collect(Collectors.toMap(
          realmKeyPair -> (String) realmKeyPair
              .get(MetadataStoreRoutingConstants.SINGLE_METADATA_STORE_REALM),
          mapEntry -> (List<String>) mapEntry.get(MetadataStoreRoutingConstants.SHARDING_KEYS)));
      return routingData;
    }
    return Collections.emptyMap();
  }
}