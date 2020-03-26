package org.apache.helix.rest.metadatastore.integration;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.rest.server.AbstractTestClass;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * TestRoutingDataUpdate tests that Helix REST server's ServerContext gets a proper update whenever
 * there is change in the routing data.
 */
public class TestRoutingDataUpdate extends AbstractTestClass {
  private static final String CLUSTER_0_SHARDING_KEY = "/TestRoutingDataUpdate-cluster-0";
  private static final String CLUSTER_1_SHARDING_KEY = "/TestRoutingDataUpdate-cluster-1";
  private final Map<String, List<String>> _routingData = new HashMap<>();

  @BeforeClass
  public void beforeClass() {
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY,
        getBaseUri().getHost());
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY,
        Integer.toString(getBaseUri().getPort()));

    // Set the multi-zk config
    System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, "true");
    // Set the MSDS address
    String msdsEndpoint = getBaseUri().toString();
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, msdsEndpoint);

    // Restart Helix Rest server to get a fresh ServerContext created
    restartRestServer();
  }

  @AfterClass
  public void afterClass() {
    // Clear all property
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_HOSTNAME_KEY);
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_PORT_KEY);
    System.clearProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);

    restartRestServer();
  }

  @Test
  public void testRoutingDataUpdate() throws Exception {
    // Set up routing data
    _routingData.put(ZK_ADDR, Arrays.asList(CLUSTER_0_SHARDING_KEY, CLUSTER_1_SHARDING_KEY));
    _routingData.put(_zkAddrTestNS, new ArrayList<>());
    String routingDataString = OBJECT_MAPPER.writeValueAsString(_routingData);
    put(MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT, null,
        Entity.entity(routingDataString, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // Need to wait so that ServerContext processes the callback
    // TODO: Think of another way to wait -
    // this is only used because of the nature of the testing environment
    // in production, the server might return a 500 if a http call comes in before callbacks get
    // processed fully
    Thread.sleep(500L);

    // Create the first cluster using Helix REST API via ClusterAccessor
    put("/clusters" + CLUSTER_0_SHARDING_KEY, null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());
    // Check that the first cluster is created in the first ZK as designated by routing data
    Assert.assertTrue(_gZkClient.exists(CLUSTER_0_SHARDING_KEY));
    Assert.assertFalse(_gZkClientTestNS.exists(CLUSTER_0_SHARDING_KEY));

    // Change the routing data mapping so that CLUSTER_1 points to the second ZK
    _routingData.clear();
    _routingData.put(ZK_ADDR, Collections.singletonList(CLUSTER_0_SHARDING_KEY));
    _routingData.put(_zkAddrTestNS, Collections.singletonList(CLUSTER_1_SHARDING_KEY));
    routingDataString = OBJECT_MAPPER.writeValueAsString(_routingData);
    put(MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT, null,
        Entity.entity(routingDataString, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // Need to wait so that ServerContext processes the callback
    // TODO: Think of another way to wait -
    // this is only used because of the nature of the testing environment
    // in production, the server might return a 500 if a http call comes in before callbacks get
    // processed fully
    Thread.sleep(500L);

    // Create the second cluster using Helix REST API via ClusterAccessor
    put("/clusters" + CLUSTER_1_SHARDING_KEY, null,
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());
    // Check that the second cluster is created in the second ZK as designated by routing data
    Assert.assertTrue(_gZkClientTestNS.exists(CLUSTER_1_SHARDING_KEY));
    Assert.assertFalse(_gZkClient.exists(CLUSTER_1_SHARDING_KEY));

    // Remove all routing data
    put(MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT, null, Entity
        .entity(OBJECT_MAPPER.writeValueAsString(Collections.emptyMap()),
            MediaType.APPLICATION_JSON_TYPE), Response.Status.CREATED.getStatusCode());

    // Need to wait so that ServerContext processes the callback
    // TODO: Think of another way to wait -
    // this is only used because of the nature of the testing environment
    // in production, the server might return a 500 if a http call comes in before callbacks get
    // processed fully
    Thread.sleep(500L);

    // Delete clusters - both should fail because routing data don't have these clusters
    delete("/clusters" + CLUSTER_0_SHARDING_KEY,
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    delete("/clusters" + CLUSTER_1_SHARDING_KEY,
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());

    // Set the routing data again
    put(MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT, null,
        Entity.entity(routingDataString, MediaType.APPLICATION_JSON_TYPE),
        Response.Status.CREATED.getStatusCode());

    // Need to wait so that ServerContext processes the callback
    // TODO: Think of another way to wait -
    // this is only used because of the nature of the testing environment
    // in production, the server might return a 500 if a http call comes in before callbacks get
    // processed fully
    Thread.sleep(500L);

    // Attempt deletion again - now they should succeed
    delete("/clusters" + CLUSTER_0_SHARDING_KEY, Response.Status.OK.getStatusCode());
    delete("/clusters" + CLUSTER_1_SHARDING_KEY, Response.Status.OK.getStatusCode());

    // Double-verify using ZkClients
    Assert.assertFalse(_gZkClientTestNS.exists(CLUSTER_1_SHARDING_KEY));
    Assert.assertFalse(_gZkClient.exists(CLUSTER_0_SHARDING_KEY));

    // Remove all routing data
    put(MetadataStoreRoutingConstants.MSDS_GET_ALL_ROUTING_DATA_ENDPOINT, null, Entity
        .entity(OBJECT_MAPPER.writeValueAsString(Collections.emptyMap()),
            MediaType.APPLICATION_JSON_TYPE), Response.Status.CREATED.getStatusCode());
  }

  private void restartRestServer() {
    if (_helixRestServer != null) {
      _helixRestServer.shutdown();
    }
    _helixRestServer = startRestServer();
  }
}
