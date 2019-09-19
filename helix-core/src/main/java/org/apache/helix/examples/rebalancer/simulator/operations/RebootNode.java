package org.apache.helix.examples.rebalancer.simulator.operations;

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

import org.apache.helix.HelixAdmin;
import org.apache.helix.examples.rebalancer.simulator.AbstractSimulator;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;

/**
 * Reset a node.
 */
public class RebootNode implements Operation {
  private final String _instanceName;
  private final AbstractSimulator _simulator;

  public RebootNode(String instanceName, AbstractSimulator simulator) {
    _instanceName = instanceName;
    _simulator = simulator;
  }

  @Override
  public boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName) {
    try {
      _simulator.resetProcess(_instanceName);
    } catch (Exception e) {
      return false;
    }
    ZkHelixClusterVerifier verifier =
        new ZkHelixClusterVerifier(zkClient.getServers(), clusterName) {
          @Override
          public boolean verifyByZkCallback(long timeout) {
            List<ClusterVerifyTrigger> triggers = new ArrayList<ClusterVerifyTrigger>();
            triggers.add(new ClusterVerifyTrigger(_keyBuilder.liveInstances(), false, true, true));
            return verifyByCallback(timeout, triggers);
          }

          @Override
          protected boolean verifyState() {
            Set<String> actualLiveNodes =
                new HashSet<String>(_accessor.getChildNames(_keyBuilder.liveInstances()));
            return actualLiveNodes.contains(_instanceName);
          }
        };
    return verifier.verify(5000);
  }

  @Override
  public String getDescription() {
    return "Reboot node: " + _instanceName;
  }
}
