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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.http.Header;
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
  private static final String AGGREGATED_HEALTH_STATUS = "/aggregatedHealthStatus";

  private static final String IS_HEALTHY_FIELD = "IS_HEALTHY";
  private static final String PARTITIONS = "partitions";
  private static final String ACCEPT_CONTENT_TYPE = "application/json";
  private static final String HEALTH_INSTANCES = "stoppableInstances";
  private static final String NON_STOPPABLE_INSTANCES = "nonStoppableInstancesWithReasons";
  private static final int MAX_REDIRECT = 3;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final HttpClient _httpClient;

  private interface JsonConverter<T> {
    Map<String, T> convert(JsonNode jsonNode);
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
    JsonConverter<Boolean> jsonConverter = this::extractBooleanMap;
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
    JsonConverter<Boolean> jsonConverter = this::extractPartitionBooleanMap;
    return handleResponse(post(url, payLoads), jsonConverter);
  }

  @Override
  public Map<String, List<String>> getAggregatedStoppableCheck(String baseUrl,
      List<String> instances, Set<String> toBeStoppedInstances, String clusterId,
      Map<String, String> customPayloads) throws IOException {
    /*
     * example url: http://<baseUrl>/aggregatedHealthStatus -d {
     * "instances" : ["n1", "n3", "n9"],
     * "to_be_stopped_instances": "["n2", "n4"]",
     * "cluster_id": "cluster1",
     * "<key>": "<value>"
     * ...
     * }
     *
     * The response will be a json object with two fields: "stoppableInstances" and
     * "nonStoppableInstancesWithReasons". The value of "stoppableInstances" is a list of instances
     * that are stoppable. The value of "nonStoppableInstancesWithReasons" is a map where the key is
     * the instance name and the value is the reasons why the instance is not stoppable.
     *
     * example response: {
     * "stoppableInstances": ["n1", "n3"],
     * "nonStoppableInstancesWithReasons": {
     * "n2": "reason1,reason2",
     * "n4": "reason3"
     * }
     */
    String url = baseUrl + AGGREGATED_HEALTH_STATUS;
    Map<String, Object> payLoads = new HashMap<>(customPayloads);
    if (instances != null && !instances.isEmpty()) {
      payLoads.put("instances", instances);
    }
    if (toBeStoppedInstances != null && !toBeStoppedInstances.isEmpty()) {
      payLoads.put("to_be_stopped_instances", toBeStoppedInstances);
    }
    if (clusterId != null) {
      payLoads.put("cluster_id", clusterId);
    }

    // JsonConverter to handle the response and map instance health status
    JsonConverter<List<String>> converter = this::extractAggregatedStatus;
    return handleResponse(post(url, payLoads), converter);
  }

  @VisibleForTesting
  protected JsonNode getJsonObject(HttpResponse httpResponse) throws IOException {
    HttpEntity httpEntity = httpResponse.getEntity();
    String str = EntityUtils.toString(httpEntity);
    LOG.info("Converting Response Content {} to JsonNode", str);
    return OBJECT_MAPPER.readTree(str);
  }

  private <T> Map<String, T> handleResponse(HttpResponse httpResponse,
      JsonConverter<T> jsonConverter) throws IOException {
    int status = httpResponse.getStatusLine().getStatusCode();
    if (status == HttpStatus.SC_OK) {
      LOG.info("Expected HttpResponse statusCode: {}", HttpStatus.SC_OK);
      return jsonConverter.convert(getJsonObject(httpResponse));
    } else {
      // Ensure entity is fully consumed so stream is closed.
      EntityUtils.consumeQuietly(httpResponse.getEntity());
      throw new ClientProtocolException(
          "Unexpected response status: " + status + ", reason: " + httpResponse.getStatusLine()
              .getReasonPhrase());
    }
  }

  @VisibleForTesting
  protected HttpResponse post(String url, Map<String, Object> payloads) throws IOException {
    HttpPost postRequest = new HttpPost(url);
    int retries = 0;
    HttpResponse response = null;

    while (retries < MAX_REDIRECT) {
      try {
        postRequest.setHeader("Accept", ACCEPT_CONTENT_TYPE);
        StringEntity entity = new StringEntity(OBJECT_MAPPER.writeValueAsString(payloads),
            ContentType.APPLICATION_JSON);
        postRequest.setEntity(entity);
        LOG.info("Executing request: {}, headers: {}, entity: {}", postRequest.getRequestLine(),
            postRequest.getAllHeaders(), postRequest.getEntity());

        // Execute the request
        response = _httpClient.execute(postRequest);
        int status = response.getStatusLine().getStatusCode();

        if (status == HttpStatus.SC_OK) {
          return response;  // Return the successful response
        } else if (status == HttpStatus.SC_MOVED_TEMPORARILY) {
          // If receiving 302 Found (redirect), handle the redirection
          Header locationHeader = response.getFirstHeader("Location");
          if (locationHeader != null) {
            String redirectUrl = locationHeader.getValue();
            LOG.info("Redirecting to: {}", redirectUrl);
            // Update the post request with the new URL
            postRequest = new HttpPost(redirectUrl);
            retries++;
            LOG.info("Retrying redirect (attempt {} of {})", retries, MAX_REDIRECT);
          } else {
            LOG.warn("Received 302 but no Location header is present, stopping retries.");
            break;  // Break out if there is no valid redirect location
          }
        } else {
          LOG.warn("Received non-200 and non-302 status code: {}, payloads: {}", status, payloads);
          return response;  // Return response without retry
        }
      } catch (IOException e) {
        // Release connection to be reused and avoid connection leakage
        postRequest.releaseConnection();
        throw e;
      }
    }

    return response;
  }

  private Map<String, Boolean> extractBooleanMap(JsonNode jsonNode) {
    Map<String, Boolean> result = new HashMap<>();
    jsonNode.fields().forEachRemaining(kv -> result.put(kv.getKey(), kv.getValue().asBoolean()));
    return result;
  }

  private Map<String, Boolean> extractPartitionBooleanMap(JsonNode jsonNode) {
    Map<String, Boolean> result = new HashMap<>();
    jsonNode.fields().forEachRemaining(
        kv -> result.put(kv.getKey(), kv.getValue().get(IS_HEALTHY_FIELD).asBoolean()));
    return result;
  }

  private Map<String, List<String>> extractAggregatedStatus(JsonNode jsonNode) {
    Map<String, List<String>> result = new HashMap<>();
    jsonNode.fields().forEachRemaining(response -> {
      String key = response.getKey();
      if (HEALTH_INSTANCES.equals(key)) {
        response.getValue().forEach(instance -> {
          result.put(instance.textValue(), Collections.emptyList());
        });
      }
      if (NON_STOPPABLE_INSTANCES.equals(key)) {
        response.getValue().fields().forEachRemaining(instance -> result.put(instance.getKey(),
            Stream.of(instance.getValue().toString().split(","))
                .collect(Collectors.toCollection(ArrayList<String>::new))));
      }
    });
    return result;
  }
}
