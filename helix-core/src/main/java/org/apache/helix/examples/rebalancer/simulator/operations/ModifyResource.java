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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;


/**
 * Modify the resource config or idealstate.
 */
public class ModifyResource implements Operation {
  private final ResourceConfig _resourceConfig;
  private final IdealState _idealState;

  public ModifyResource(ResourceConfig resourceConfig, IdealState idealState) {
    _resourceConfig = resourceConfig;
    _idealState = idealState;
  }

  @Override
  public boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName) {
    if (_resourceConfig != null) {
      ConfigAccessor accessor = new ConfigAccessor(zkClient);
      ResourceConfig resourceConfig =
          accessor.getResourceConfig(clusterName, _resourceConfig.getResourceName());
      resourceConfig.getRecord().merge(_resourceConfig.getRecord());
      accessor.setResourceConfig(clusterName, _resourceConfig.getResourceName(), resourceConfig);
    }
    if (_idealState != null) {
      IdealState is = admin.getResourceIdealState(clusterName, _idealState.getResourceName());
      is.getRecord().merge(_idealState.getRecord());
      admin.setResourceIdealState(clusterName, _idealState.getResourceName(), is);
    }
    return true;
  }

  @Override
  public String getDescription() {
    return "Modify configs of the resource: " + _idealState.getResourceName();
  }
}
