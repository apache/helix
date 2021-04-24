package org.apache.helix.controller.rebalancer.waged.model;

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
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.InstanceConfig;

import static org.mockito.Mockito.when;


public class ClusterModelTestHelper extends AbstractTestClusterModel {
  public static final String TEST_INSTANCE_ID_1 = "TestInstanceId1";
  public static final String TEST_INSTANCE_ID_2 = "TestInstanceId2";

  public ClusterModel getDefaultClusterModel() throws IOException {
    initialize();
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignableReplicas = generateReplicas(testCache);
    Set<AssignableNode> assignableNodes = generateNodes(testCache);

    ClusterContext context =
        new ClusterContext(assignableReplicas, assignableNodes, Collections.emptyMap(), Collections.emptyMap());
    return new ClusterModel(context, assignableReplicas, assignableNodes);
  }

  public ClusterModel getMultiNodeClusterModel() throws IOException {
    initialize();
    ResourceControllerDataProvider testCache = setupClusterDataCacheForNearFullUtil();
    InstanceConfig testInstanceConfig1 = createMockInstanceConfig(TEST_INSTANCE_ID_1);
    InstanceConfig testInstanceConfig2 = createMockInstanceConfig(TEST_INSTANCE_ID_2);
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    instanceConfigMap.put(TEST_INSTANCE_ID_1, testInstanceConfig1);
    instanceConfigMap.put(TEST_INSTANCE_ID_2, testInstanceConfig2);
    when(testCache.getInstanceConfigMap()).thenReturn(instanceConfigMap);
    Set<AssignableReplica> assignableReplicas = generateReplicas(testCache);
    Set<AssignableNode> assignableNodes = generateNodes(testCache);

    ClusterContext context =
        new ClusterContext(assignableReplicas, assignableNodes, Collections.emptyMap(), Collections.emptyMap());
    return new ClusterModel(context, assignableReplicas, assignableNodes);
  }
}
