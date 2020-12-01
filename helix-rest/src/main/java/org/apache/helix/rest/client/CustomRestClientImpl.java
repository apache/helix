package org.apache.helix.rest.client;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CustomRestClientImpl implements CustomRestClient {
  private static final Logger LOG = LoggerFactory.getLogger(CustomRestClientImpl.class);

  // postfix used to append at the end of base url
  private static final String INSTANCE_HEALTH_STATUS = "/instanceHealthStatus";
  private static final String PARTITION_HEALTH_STATUS = "/partitionHealthStatus";

  private static final String IS_HEALTHY_FIELD = "IS_HEALTHY";
  private static final String PARTITIONS = "partitions";
  private static final String ACCEPT_CONTENT_TYPE = "application/json";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private HttpClient _httpClient;

  private interface JsonConverter {
    Map<String, Boolean> convert(JsonNode jsonNode);
  }

  public CustomRestClientImpl(HttpClient httpClient) {
    _httpClient = httpClient;
  }

  @Override
  public Map<String, Boolean> getInstanceStoppableCheck(String baseUrl,
      Map<String, String> customPayloads) throws IOException {
    // example url: http://<baseUrl>/instanceHealthStatus, assuming the base url already directly
    // queries at the instance
    String url = baseUrl + INSTANCE_HEALTH_STATUS;
    JsonConverter jsonConverter = jsonNode -> {
      Map<String, Boolean> result = new HashMap<>();
      jsonNode.fields().forEachRemaining(kv -> result.put(kv.getKey(), kv.getValue().asBoolean()));
      return result;
    };
    return handleResponse(post(url, Collections.unmodifiableMap(customPayloads)), jsonConverter);
  }

  @Override
  public Map<String, Boolean> getPartitionStoppableCheck(String baseUrl, List<String> partitions,
      Map<String, String> customPayloads) throws IOException {
    /*
     * example url: http://<baseUrl>/partitionHealthStatus -d {
     * "partitions" : ["p1", "p3", "p9"],
     * "<key>": "<value>",
     * ...
     * }
     */
    String url = baseUrl + PARTITION_HEALTH_STATUS;
    // To avoid ImmutableMap as parameter
    Map<String, Object> payLoads = new HashMap<>(customPayloads);
    // Add the entry: "partitions" : ["p1", "p3", "p9"]
    if (partitions != null) {
      payLoads.put(PARTITIONS, partitions);
    }
    JsonConverter jsonConverter = jsonNode -> {
      Map<String, Boolean> result = new HashMap<>();
      jsonNode.fields().forEachRemaining(
          kv -> result.put(kv.getKey(), kv.getValue().get(IS_HEALTHY_FIELD).asBoolean()));
      return result;
    };
    return handleResponse(post(url, payLoads), jsonConverter);
  }

  @VisibleForTesting
  protected JsonNode getJsonObject(HttpResponse httpResponse) throws IOException {
    HttpEntity httpEntity = httpResponse.getEntity();
    String str = EntityUtils.toString(httpEntity);
    LOG.info("Converting Response Content {} to JsonNode", str);
    return OBJECT_MAPPER.readTree(str);
  }

  private Map<String, Boolean> handleResponse(HttpResponse httpResponse,
      JsonConverter jsonConverter) throws IOException {
    int status = httpResponse.getStatusLine().getStatusCode();
    if (status == HttpStatus.SC_OK) {
      LOG.info("Expected HttpResponse statusCode: {}", HttpStatus.SC_OK);
      return jsonConverter.convert(getJsonObject(httpResponse));
    } else {
      // Ensure entity is fully consumed so stream is closed.
      EntityUtils.consumeQuietly(httpResponse.getEntity());
      throw new ClientProtocolException("Unexpected response status: " + status + ", reason: "
          + httpResponse.getStatusLine().getReasonPhrase());
    }
  }

  @VisibleForTesting
  protected HttpResponse post(String url, Map<String, Object> payloads) throws IOException {
    HttpPost postRequest = new HttpPost(url);
    try {
      postRequest.setHeader("Accept", ACCEPT_CONTENT_TYPE);
      StringEntity entity = new StringEntity(OBJECT_MAPPER.writeValueAsString(payloads),
          ContentType.APPLICATION_JSON);
      postRequest.setEntity(entity);
      LOG.info("Executing request: {}, headers: {}, entity: {}", postRequest.getRequestLine(),
          postRequest.getAllHeaders(), postRequest.getEntity());

      HttpResponse response = _httpClient.execute(postRequest);
      int status = response.getStatusLine().getStatusCode();
      if (status != HttpStatus.SC_OK) {
        LOG.warn("Received non-200 status code: {}, payloads: {}", status, payloads);
      }

      return response;
    } catch (IOException e) {
      LOG.error("Failed to perform customized health check. Is participant endpoint {} available?",
          url, e);
      // Release connection to be reused and avoid connection leakage.
      postRequest.releaseConnection();
      throw e;
    }
  }
}
