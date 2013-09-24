package org.apache.helix.controller.rebalancer.context;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.model.ResourceAssignment;

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

/**
 * Allows one to come up with custom implementation of a rebalancer.<br/>
 * This will be invoked on all changes that happen in the cluster.<br/>
 * Simply return the resource assignment for a resource in this method.<br/>
 */
public interface Rebalancer {

  public void init(HelixManager helixManager);

  public ResourceAssignment computeResourceMapping(RebalancerConfig rebalancerConfig, Cluster cluster,
      ResourceCurrentState currentState);
}
