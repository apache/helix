package org.apache.helix.tools.ClusterVerifiers;

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;


public class ClusterLiveNodesVerifier extends ZkHelixClusterVerifier {

  final Set<String> _expectLiveNodes;

  @Deprecated
  public ClusterLiveNodesVerifier(RealmAwareZkClient zkclient, String clusterName,
      List<String> expectLiveNodes) {
    super(zkclient, clusterName);
    _expectLiveNodes = new HashSet<>(expectLiveNodes);
  }

  private ClusterLiveNodesVerifier(RealmAwareZkClient zkClient, String clusterName,
      Set<String> expectLiveNodes) {
    super(zkClient, clusterName);
    _expectLiveNodes = expectLiveNodes == null ? new HashSet<>() : new HashSet<>(expectLiveNodes);
  }

  @Override
  public boolean verifyByZkCallback(long timeout) {
    List<ClusterVerifyTrigger> triggers = new ArrayList<ClusterVerifyTrigger>();
    triggers.add(new ClusterVerifyTrigger(_keyBuilder.liveInstances(), false, true, true));

    return verifyByCallback(timeout, triggers);
  }

  @Override
  protected boolean verifyState() throws Exception {
    Set<String> actualLiveNodes =
        new HashSet<String>(_accessor.getChildNames(_keyBuilder.liveInstances()));
    return _expectLiveNodes.equals(actualLiveNodes);
  }

  @Override
  public void finalize() {
    close();
  }

  public static class Builder extends ZkHelixClusterVerifier.Builder<Builder> {
    private final String _clusterName; // This is the ZK path sharding key
    private final Set<String> _expectLiveNodes;

    public Builder(String clusterName, Set<String> expectLiveNodes) {
      _clusterName = clusterName;
      _expectLiveNodes = expectLiveNodes;
    }

    public ClusterLiveNodesVerifier build() {
      if (_clusterName == null || _clusterName.isEmpty()) {
        throw new IllegalArgumentException("Cluster name is missing!");
      }

      validate();
      return new ClusterLiveNodesVerifier(
          createZkClient(RealmAwareZkClient.RealmMode.SINGLE_REALM, _realmAwareZkConnectionConfig,
              _realmAwareZkClientConfig, _zkAddress), _clusterName, _expectLiveNodes);
    }
  }
}
