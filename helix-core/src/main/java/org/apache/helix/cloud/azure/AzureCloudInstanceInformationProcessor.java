package org.apache.helix.cloud.azure;

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
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.HelixCloudProperty;
import org.apache.helix.HelixException;
import org.apache.helix.api.cloud.CloudInstanceInformationProcessor;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureCloudInstanceInformationProcessor
    implements CloudInstanceInformationProcessor<String> {
  private static final Logger LOG =
      LoggerFactory.getLogger(AzureCloudInstanceInformationProcessor.class);
  private final CloseableHttpClient _closeableHttpClient;
  private final HelixCloudProperty _helixCloudProperty;
  private static final String COMPUTE = "compute";
  private static final String INSTANCE_NAME = "name";
  private static final String DOMAIN = "platformFaultDomain";
  private static final String INSTANCE_SET_NAME = "vmScaleSetName";

  public AzureCloudInstanceInformationProcessor(HelixCloudProperty helixCloudProperty) {
    _helixCloudProperty = helixCloudProperty;

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout((int) helixCloudProperty.getCloudRequestTimeout())
        .setConnectTimeout((int) helixCloudProperty.getCloudConnectionTimeout()).build();

    HttpRequestRetryHandler httpRequestRetryHandler =
        (IOException exception, int executionCount, HttpContext context) -> {
          LOG.warn("Execution count: " + executionCount + ".", exception);
          return !(executionCount >= helixCloudProperty.getCloudMaxRetry()
              || exception instanceof InterruptedIOException
              || exception instanceof UnknownHostException || exception instanceof SSLException);
        };

    //TODO: we should regularize the way how httpClient should be used throughout Helix. e.g. Helix-rest could also use in the same way
    _closeableHttpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig)
        .setRetryHandler(httpRequestRetryHandler).build();
  }

  /**
   * This constructor is for unit test purpose only.
   * User could provide helixCloudProperty and a mocked http client to test the functionality of
   * this class.
   */
  public AzureCloudInstanceInformationProcessor(HelixCloudProperty helixCloudProperty,
      CloseableHttpClient closeableHttpClient) {
    _helixCloudProperty = helixCloudProperty;
    _closeableHttpClient = closeableHttpClient;
  }

  /**
   * Fetch raw Azure cloud instance information based on the urls provided
   * @return raw Azure cloud instance information
   */
  @Override
  public List<String> fetchCloudInstanceInformation() {
    List<String> response = new ArrayList<>();
    for (String url : _helixCloudProperty.getCloudInfoSources()) {
      response.add(getAzureCloudInformationFromUrl(url));
    }
    return response;
  }

  /**
   * Query Azure Instance Metadata Service to get the instance(VM) information
   * @return raw Azure cloud instance information
   */
  private String getAzureCloudInformationFromUrl(String url) {
    HttpGet httpGet = new HttpGet(url);
    httpGet.setHeader("Metadata", "true");

    try {
      CloseableHttpResponse response = _closeableHttpClient.execute(httpGet);
      if (response == null || response.getStatusLine().getStatusCode() != 200) {
        String errorMsg = String.format(
            "Failed to get an HTTP Response for the request. Response: %s. Status code: %s",
            (response == null ? "NULL" : response.getStatusLine().getReasonPhrase()),
            response.getStatusLine().getStatusCode());
        throw new HelixException(errorMsg);
      }
      String responseString = EntityUtils.toString(response.getEntity());
      LOG.info("VM instance information query result: {}", responseString);
      return responseString;
    } catch (IOException e) {
      throw new HelixException(
          String.format("Failed to get Azure cloud instance information from url %s", url), e);
    }
  }

  /**
   * Parse raw Azure cloud instance information.
   * @return required azure cloud instance information
   */
  @Override
  public AzureCloudInstanceInformation parseCloudInstanceInformation(List<String> responses) {
    AzureCloudInstanceInformation azureCloudInstanceInformation = null;
    if (responses.size() > 1) {
      throw new HelixException("Multiple responses are not supported for Azure now");
    }
    String response = responses.get(0);
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode jsonNode = mapper.readTree(response);
      JsonNode computeNode = jsonNode.path(COMPUTE);
      if (!computeNode.isMissingNode()) {
        String vmName = computeNode.path(INSTANCE_NAME).textValue();
        String platformFaultDomain = computeNode.path(DOMAIN).textValue();
        String vmssName = computeNode.path(INSTANCE_SET_NAME).textValue();
        String azureTopology = AzureConstants.AZURE_TOPOLOGY;
        String[] parts = azureTopology.trim().split("/");
        //The hostname will be filled in by each participant
        String domain = parts[1] + "=" + platformFaultDomain + "," + parts[2] + "=";

        AzureCloudInstanceInformation.Builder builder = new AzureCloudInstanceInformation.Builder();
        builder.setInstanceName(vmName).setFaultDomain(domain).setInstanceSetName(vmssName);
        azureCloudInstanceInformation = builder.build();
      }
    } catch (IOException e) {
      throw new HelixException(
          String.format("Error in parsing cloud instance information: %s", response), e);
    }
    return azureCloudInstanceInformation;
  }
}
