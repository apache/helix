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
import java.util.stream.Collectors;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

class CustomRestClientImpl implements CustomRestClient {
  private static final Logger LOG = LoggerFactory.getLogger(CustomRestClient.class);

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

  /**
   * TODO: create Config to initialize SSLContext for Https endpoint
   * Override the constructor if https endpoint is expected
   */
  public CustomRestClientImpl() {
    _httpClient = HttpClients.createDefault();
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
      jsonNode.fields()
          .forEachRemaining(kv -> result.put(kv.getKey(), kv.getValue().asBoolean()));
      return result;
    };
    return handleResponse(post(url, customPayloads), jsonConverter);
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
    Map<String, String> payLoads = new HashMap<>(customPayloads);
    // Add the entry: "partitions" : ["p1", "p3", "p9"]
    payLoads.put(PARTITIONS, partitions.toString());
    JsonConverter jsonConverter = jsonNode -> {
      Map<String, Boolean> result = new HashMap<>();
      jsonNode.fields()
          .forEachRemaining(kv -> result.put(kv.getKey(), kv.getValue().get(IS_HEALTHY_FIELD).asBoolean()));
      return result;
    };
    return handleResponse(post(url, payLoads), jsonConverter);
  }

  @VisibleForTesting
  protected JsonNode getJsonObject(HttpResponse httpResponse) throws IOException {
    HttpEntity httpEntity = httpResponse.getEntity();
    String str = EntityUtils.toString(httpEntity);
    return OBJECT_MAPPER.readTree(str);
  }

  private Map<String, Boolean> handleResponse(HttpResponse httpResponse,
      JsonConverter jsonConverter) throws IOException {
    int status = httpResponse.getStatusLine().getStatusCode();
    if (status == 200) {
      return jsonConverter.convert(getJsonObject(httpResponse));
    } else {
      throw new ClientProtocolException("Unexpected response status: " + status + ", reason: "
          + httpResponse.getStatusLine().getReasonPhrase());
    }
  }

  private HttpResponse post(String url, Map<String, String> payloads) throws IOException {
    List<NameValuePair> params = payloads.entrySet().stream()
        .map(entry -> new BasicNameValuePair(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
    try {
      HttpPost postRequest = new HttpPost(url);
      postRequest.setHeader("Accept", ACCEPT_CONTENT_TYPE);
      postRequest.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
      LOG.info("Executing request {}", postRequest.getRequestLine());
      return _httpClient.execute(postRequest);
    } catch (IOException e) {
      LOG.error("Failed to perform customized health check. Is participant endpoint {} available?",
          url, e);
      throw e;
    }
  }
}