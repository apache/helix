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
import java.util.HashSet;
import java.util.Set;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;

public class ClusterModelTestHelper extends AbstractTestClusterModel {

  public ClusterModel getDefaultClusterModel() throws IOException {
    initialize();
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignableReplicas = generateReplicas(testCache);
    Set<AssignableNode> assignableNodes = generateNodes(testCache);

    ClusterContext context =
        new ClusterContext(assignableReplicas, 2, Collections.emptyMap(), Collections.emptyMap());
    return new ClusterModel(context, assignableReplicas, assignableNodes);
  }

  private Set<AssignableNode> generateNodes(ResourceControllerDataProvider testCache) {
    Set<AssignableNode> nodeSet = new HashSet<>();
    testCache.getInstanceConfigMap().values().stream()
            .forEach(config -> nodeSet.add(new AssignableNode(testCache.getClusterConfig(),
                    testCache.getInstanceConfigMap().get(_testInstanceId), config.getInstanceName())));
    return nodeSet;
  }
}
