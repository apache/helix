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

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.examples.rebalancer.simulator.AbstractSimulator;
import org.apache.helix.manager.zk.client.HelixZkClient;


/**
 * Drop a node from the cluster.
 */
public class RemoveNode implements Operation {
  private final String _instanceName;
  private final AbstractSimulator _simulator;

  public RemoveNode(String instanceName, AbstractSimulator simulator) {
    _instanceName = instanceName;
    _simulator = simulator;
  }

  @Override
  public boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName) {
    _simulator.stopProcess(_instanceName);
    admin.dropInstance(clusterName, admin.getInstanceConfig(clusterName, _instanceName));
    try {
      admin.getInstanceConfig(clusterName, _instanceName);
      return false;
    } catch (HelixException ex) {
      _simulator.getNodeToZoneMap().remove(_instanceName);
      return true;
    }
  }

  @Override
  public String getDescription() {
    return "Remove node: " + _instanceName;
  }
}
