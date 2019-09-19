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
import org.apache.helix.manager.zk.client.HelixZkClient;

/**
 * Interface for the Helix administrate operations.
 */
public interface Operation {
  /**
   * Execute the operation.
   *
   * @param admin       helix admin object.
   * @param zkClient    the ZK client that connects to the simulation ZkServers.
   * @param clusterName the simulation cluter name.
   * @return true if the operation is done successfully.
   */
  boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName);

  /**
   * @return The description of the operation.
   */
  String getDescription();
}
