package org.apache.helix.rest.metadatastore.accessor;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.metadatastore.ZkMetadataStoreDirectory;
import org.apache.helix.rest.metadatastore.concurrency.ZkDistributedLeaderElection;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkRoutingDataWriter implements MetadataStoreRoutingDataWriter {
  // Time out for http requests that are forwarded to leader instances measured in milliseconds
  private static final int HTTP_REQUEST_FORWARDING_TIMEOUT = 60 * 1000;
  private static final Logger LOG = LoggerFactory.getLogger(ZkRoutingDataWriter.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String SIMPLE_FIELD_KEY_HOSTNAME = "hostname";
  private static final String SIMPLE_FIELD_KEY_PORT = "port";
  private static final String SIMPLE_FIELD_KEY_CONTEXT_URL_PREFIX = "contextUrlPrefix";

  private final String _namespace;
  private final HelixZkClient _zkClient;
  private final ZkDistributedLeaderElection _leaderElection;
  private final CloseableHttpClient _forwardHttpClient;
  private final String _myHostName;

  public ZkRoutingDataWriter(String namespace, String zkAddress) {
    if (namespace == null || namespace.isEmpty()) {
      throw new IllegalArgumentException("namespace cannot be null or empty!");
    }
    _namespace = namespace;
    if (zkAddress == null || zkAddress.isEmpty()) {
      throw new IllegalArgumentException("Zk address cannot be null or empty!");
    }
    _zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
            new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));

    ZkMetadataStoreDirectory.createRoutingDataPath(_zkClient, zkAddress);

    // Get the hostname (REST endpoint) from System property
    String hostName = System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY);
    if (hostName == null || hostName.isEmpty()) {
      String errMsg =
          "ZkRoutingDataWriter: Hostname is not set or is empty. System.getProperty fails to fetch "
              + MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY;
      LOG.error(errMsg);
      throw new IllegalStateException(errMsg);
    }
    _myHostName = HttpConstants.HTTP_PROTOCOL_PREFIX + hostName;

    ZNRecord myServerInfo = new ZNRecord(hostName);
    myServerInfo.setSimpleField(SIMPLE_FIELD_KEY_HOSTNAME, hostName);

    String port = System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY);
    if (port != null && !port.isEmpty()) {
      myServerInfo.setSimpleField(SIMPLE_FIELD_KEY_PORT, port);
    }

    // One example of context url prefix is "/admin/v2". With the prefix specified, we want to
    // make sure the final url is "/admin/v2/namespaces/NAMESPACE/some/endpoint"; without it
    // being specified, we will skip it and go with "/namespaces/NAMESPACE/some/endpoint".
    String contextUrlPrefix =
        System.getProperty(MetadataStoreRoutingConstants.MSDS_CONTEXT_URL_PREFIX_KEY);
    if (contextUrlPrefix != null && !contextUrlPrefix.isEmpty()) {
      myServerInfo.setSimpleField(SIMPLE_FIELD_KEY_CONTEXT_URL_PREFIX, contextUrlPrefix);
    }

    _leaderElection = new ZkDistributedLeaderElection(_zkClient,
        MetadataStoreRoutingConstants.LEADER_ELECTION_ZNODE, myServerInfo);

    RequestConfig config = RequestConfig.custom().setConnectTimeout(HTTP_REQUEST_FORWARDING_TIMEOUT)
        .setConnectionRequestTimeout(HTTP_REQUEST_FORWARDING_TIMEOUT)
        .setSocketTimeout(HTTP_REQUEST_FORWARDING_TIMEOUT).build();
    _forwardHttpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
  }

  public static String buildEndpointFromLeaderElectionNode(ZNRecord znRecord) {
    List<String> urlComponents =
        new ArrayList<>(Collections.singletonList(HttpConstants.HTTP_PROTOCOL_PREFIX));
    urlComponents.add(znRecord.getSimpleField(SIMPLE_FIELD_KEY_HOSTNAME));
    String port = znRecord.getSimpleField(SIMPLE_FIELD_KEY_PORT);
    if (port != null && !port.isEmpty()) {
      urlComponents.add(":");
      urlComponents.add(port);
    }
    String contextUrlPrefix = znRecord.getSimpleField(SIMPLE_FIELD_KEY_CONTEXT_URL_PREFIX);
    if (contextUrlPrefix != null && !contextUrlPrefix.isEmpty()) {
      urlComponents.add(contextUrlPrefix);
    }
    return String.join("", urlComponents);
  }

  @Override
  public synchronized boolean addMetadataStoreRealm(String realm) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      return createZkRealm(realm);
    }

    String urlSuffix =
        constructUrlSuffix(MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, realm);
    return buildAndSendRequestToLeader(urlSuffix, HttpConstants.RestVerbs.PUT,
        Response.Status.CREATED.getStatusCode());
  }

  @Override
  public synchronized boolean deleteMetadataStoreRealm(String realm) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      return deleteZkRealm(realm);
    }

    String urlSuffix =
        constructUrlSuffix(MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, realm);
    return buildAndSendRequestToLeader(urlSuffix, HttpConstants.RestVerbs.DELETE,
        Response.Status.OK.getStatusCode());
  }

  @Override
  public synchronized boolean addShardingKey(String realm, String shardingKey) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      return createZkShardingKey(realm, shardingKey);
    }

    String urlSuffix =
        constructUrlSuffix(MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, realm,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_SHARDING_KEYS_ENDPOINT, shardingKey);
    return buildAndSendRequestToLeader(urlSuffix, HttpConstants.RestVerbs.PUT,
        Response.Status.CREATED.getStatusCode());
  }

  @Override
  public synchronized boolean deleteShardingKey(String realm, String shardingKey) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      return deleteZkShardingKey(realm, shardingKey);
    }

    String urlSuffix =
        constructUrlSuffix(MetadataStoreRoutingConstants.MSDS_GET_ALL_REALMS_ENDPOINT, realm,
            MetadataStoreRoutingConstants.MSDS_GET_ALL_SHARDING_KEYS_ENDPOINT, shardingKey);
    return buildAndSendRequestToLeader(urlSuffix, HttpConstants.RestVerbs.DELETE,
        Response.Status.OK.getStatusCode());
  }

  @Override
  public synchronized boolean setRoutingData(Map<String, List<String>> routingData) {
    if (_leaderElection.isLeader()) {
      if (_zkClient.isClosed()) {
        throw new IllegalStateException("ZkClient is closed!");
      }
      if (routingData == null) {
        throw new IllegalArgumentException("routingData given is null!");
      }

      // Remove existing routing data
      for (String zkRealm : _zkClient
          .getChildren(MetadataStoreRoutingConstants.ROUTING_DATA_PATH)) {
        if (!_zkClient.delete(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm)) {
          LOG.error(
              "Failed to delete existing routing data in setRoutingData()! Namespace: {}, Realm: {}",
              _namespace, zkRealm);
          return false;
        }
      }

      // For each ZkRealm, write the given routing data to ZooKeeper
      for (Map.Entry<String, List<String>> routingDataEntry : routingData.entrySet()) {
        String zkRealm = routingDataEntry.getKey();
        List<String> shardingKeyList = routingDataEntry.getValue();

        ZNRecord znRecord = new ZNRecord(zkRealm);
        znRecord
            .setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY, shardingKeyList);

        String realmPath = MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + zkRealm;
        try {
          if (!_zkClient.exists(realmPath)) {
            _zkClient.createPersistent(realmPath);
          }
          _zkClient.writeData(realmPath, znRecord);
        } catch (Exception e) {
          LOG.error("Failed to write data in setRoutingData()! Namespace: {}, Realm: {}",
              _namespace, zkRealm, e);
          return false;
        }
      }
      return true;
    }

    String url = buildEndpointFromLeaderElectionNode(_leaderElection.getCurrentLeaderInfo())
        + constructUrlSuffix(MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT);
    HttpPut httpPut = new HttpPut(url);
    String routingDataJsonString;
    try {
      routingDataJsonString = OBJECT_MAPPER.writeValueAsString(routingData);
    } catch (JsonGenerationException | JsonMappingException e) {
      throw new IllegalArgumentException(e.getMessage());
    } catch (IOException e) {
      LOG.error(
          "setRoutingData failed before forwarding the request to leader: an exception happened while routingData is converted to json. routingData: {}",
          routingData, e);
      return false;
    }
    httpPut.setEntity(new StringEntity(routingDataJsonString, ContentType.APPLICATION_JSON));
    return sendRequestToLeader(httpPut, Response.Status.CREATED.getStatusCode());
  }

  @Override
  public synchronized void close() {
    _zkClient.close();
    try {
      _forwardHttpClient.close();
    } catch (IOException e) {
      LOG.error("HttpClient failed to close. ", e);
    }
  }

  /**
   * Creates a ZK realm ZNode and populates it with an empty ZNRecord if it doesn't exist already.
   * @param realm
   * @return
   */
  protected boolean createZkRealm(String realm) {
    if (_zkClient.exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm)) {
      LOG.warn("createZkRealm() called for realm: {}, but this realm already exists! Namespace: {}",
          realm, _namespace);
      return true;
    }
    try {
      _zkClient.createPersistent(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm);
      _zkClient.writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm,
          new ZNRecord(realm));
    } catch (Exception e) {
      LOG.error("Failed to create ZkRealm: {}, Namespace: {}", realm, _namespace, e);
      return false;
    }

    return true;
  }

  protected boolean deleteZkRealm(String realm) {
    if (!_zkClient.exists(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm)) {
      LOG.warn(
          "deleteZkRealm() called for realm: {}, but this realm already doesn't exist! Namespace: {}",
          realm, _namespace);
      return true;
    }
    return _zkClient.delete(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm);
  }

  protected boolean createZkShardingKey(String realm, String shardingKey) {
    // If the realm does not exist already, then create the realm
    String realmPath = MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm;
    if (!_zkClient.exists(realmPath)) {
      // Create the realm
      if (!createZkRealm(realm)) {
        // Failed to create the realm - log and return false
        LOG.error(
            "Failed to add sharding key because ZkRealm creation failed! Namespace: {}, Realm: {}, Sharding key: {}",
            _namespace, realm, shardingKey);
        return false;
      }
    }

    ZNRecord znRecord;
    try {
      znRecord = _zkClient.readData(realmPath);
    } catch (Exception e) {
      LOG.error(
          "Failed to read the realm ZNRecord in addShardingKey()! Namespace: {}, Realm: {}, ShardingKey: {}",
          _namespace, realm, shardingKey, e);
      return false;
    }
    List<String> shardingKeys =
        znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY);
    if (shardingKeys == null || shardingKeys.isEmpty()) {
      shardingKeys = new ArrayList<>();
    }
    shardingKeys.add(shardingKey);
    znRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY, shardingKeys);
    try {
      _zkClient.writeData(realmPath, znRecord);
    } catch (Exception e) {
      LOG.error(
          "Failed to write the realm ZNRecord in addShardingKey()! Namespace: {}, Realm: {}, ShardingKey: {}",
          _namespace, realm, shardingKey, e);
      return false;
    }
    return true;
  }

  protected boolean deleteZkShardingKey(String realm, String shardingKey) {
    ZNRecord znRecord =
        _zkClient.readData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm, true);
    if (znRecord == null || !znRecord
        .getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .contains(shardingKey)) {
      // This realm does not exist or shardingKey doesn't exist. Return true!
      return true;
    }
    znRecord.getListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY)
        .remove(shardingKey);
    // Overwrite this ZNRecord with the sharding key removed
    try {
      _zkClient.writeData(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm, znRecord);
    } catch (Exception e) {
      LOG.error(
          "Failed to write the data back in deleteShardingKey()! Namespace: {}, Realm: {}, ShardingKey: {}",
          _namespace, realm, shardingKey, e);
      return false;
    }
    return true;
  }

  private String constructUrlSuffix(String... urlParams) {
    List<String> allUrlParameters = new ArrayList<>(
        Arrays.asList(MetadataStoreRoutingConstants.MSDS_NAMESPACES_URL_PREFIX, "/", _namespace));
    for (String urlParam : urlParams) {
      if (urlParam.charAt(0) != '/') {
        urlParam = "/" + urlParam;
      }
      allUrlParameters.add(urlParam);
    }
    return String.join("", allUrlParameters);
  }

  private boolean buildAndSendRequestToLeader(String urlSuffix,
      HttpConstants.RestVerbs requestMethod, int expectedResponseCode)
      throws IllegalArgumentException {
    String url =
        buildEndpointFromLeaderElectionNode(_leaderElection.getCurrentLeaderInfo()) + urlSuffix;
    HttpUriRequest request;
    switch (requestMethod) {
      case PUT:
        request = new HttpPut(url);
        break;
      case DELETE:
        request = new HttpDelete(url);
        break;
      default:
        throw new IllegalArgumentException("Unsupported requestMethod: " + requestMethod.name());
    }

    return sendRequestToLeader(request, expectedResponseCode);
  }

  // Set to be protected for testing purposes
  protected boolean sendRequestToLeader(HttpUriRequest request, int expectedResponseCode) {
    try {
      HttpResponse response = _forwardHttpClient.execute(request);
      if (response.getStatusLine().getStatusCode() != expectedResponseCode) {
        HttpEntity respEntity = response.getEntity();
        String errorLog = "The forwarded request to leader has failed. Uri: " + request.getURI()
            + ". Error code: " + response.getStatusLine().getStatusCode() + " Current hostname: "
            + _myHostName;
        if (respEntity != null) {
          errorLog += " Response: " + EntityUtils.toString(respEntity);
        }
        LOG.error(errorLog);
        return false;
      }
    } catch (IOException e) {
      LOG.error(
          "The forwarded request to leader raised an exception. Uri: {} Current hostname: {} ",
          request.getURI(), _myHostName, e);
      return false;
    }
    return true;
  }
}
