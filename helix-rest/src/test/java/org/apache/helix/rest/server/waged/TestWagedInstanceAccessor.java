package org.apache.helix.rest.server.waged;

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
import java.util.Arrays;
import java.util.Map;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.AbstractTestClass;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestWagedInstanceAccessor extends AbstractTestClass {

  private final static String CLUSTER_NAME = "TestCluster_0";

  @BeforeClass
  public void beforeClass() {
    // Set up WAGED rebalancer specific configs
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setInstanceCapacityKeys(Arrays.asList("FOO", "BAR"));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  @Test
  public void testValidateInstance()
      throws IOException {
    // Pick an instance from the cluster to validate
    String instanceToValidate = _instancesMap.get(CLUSTER_NAME).iterator().next();

    // This should fail because none of the instances have weight configured
    get("clusters/" + CLUSTER_NAME + "/waged/instances/" + instanceToValidate + "/validate", null,
        Response.Status.BAD_REQUEST.getStatusCode(), true);

    // Set the weight configuration for this instance
    Map<String, Integer> instanceCapacityMap = ImmutableMap.of("FOO", 1000, "BAR", 1000);
    InstanceConfig instanceConfig =
        _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceToValidate);
    instanceConfig.setInstanceCapacityMap(instanceCapacityMap);
    _configAccessor.setInstanceConfig(CLUSTER_NAME, instanceToValidate, instanceConfig);

    String body =
        get("clusters/" + CLUSTER_NAME + "/waged/instances/" + instanceToValidate + "/validate",
            null, Response.Status.OK.getStatusCode(), true);
    JsonNode node = OBJECT_MAPPER.readTree(body);
    Assert.assertEquals(node.get(instanceToValidate).toString(), "true");
  }
}
