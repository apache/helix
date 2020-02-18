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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;


public class HttpRoutingDataReader {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * This class is a Singleton.
   */
  private HttpRoutingDataReader() {
  }

  /**
   * Fetches routing data from the data source via HTTP.
   * @return a mapping from "metadata store realm addresses" to lists of "metadata store sharding keys", where the sharding keys in a value list all route to the realm address in the key disallows a meaningful mapping to be returned
   */
  public static Map<String, List<String>> getRoutingData(String msdsEndpoint) {

    Map<String, List<String>> rawRoutingData = new HashMap<>();

    HttpGet requestAllRealmNames =
        new HttpGet(msdsEndpoint); //TODO: construct an endpoint once REST endpoint is finalized
    try (CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(requestAllRealmNames)) {

      HttpEntity entity = response.getEntity();
      // return it as a String
      if (entity != null) {
        String result = EntityUtils.toString(entity);
        List<String> realmNames = OBJECT_MAPPER.readValue(result, List.class);
        for (String realmName : realmNames) {
          HttpGet requestAllShardingKeys =
              new HttpGet(msdsEndpoint); // TODO: construct the right endpoint for sharding keys
          CloseableHttpResponse shardingKeyResponse = httpClient.execute(requestAllShardingKeys);
          String shardingKeyString = EntityUtils.toString(shardingKeyResponse.getEntity());
          List<String> shardingKeys = OBJECT_MAPPER.readValue(shardingKeyString, List.class);
          rawRoutingData.put(realmName, shardingKeys);
        }
      }
    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return rawRoutingData;
  }
}
