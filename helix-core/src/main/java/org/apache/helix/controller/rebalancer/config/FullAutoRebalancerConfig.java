package org.apache.helix.controller.rebalancer.config;

import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.rebalancer.FullAutoRebalancer;
import org.apache.helix.controller.rebalancer.RebalancerRef;
import org.apache.helix.model.IdealState.RebalanceMode;

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
 * RebalancerConfig for FULL_AUTO rebalancing mode. By default, it corresponds to
 * {@link FullAutoRebalancer}
 */
public class FullAutoRebalancerConfig extends PartitionedRebalancerConfig {
  public FullAutoRebalancerConfig() {
    if (getClass().equals(FullAutoRebalancerConfig.class)) {
      // only mark this as full auto mode if this specifc config is used
      setRebalanceMode(RebalanceMode.FULL_AUTO);
    } else {
      setRebalanceMode(RebalanceMode.USER_DEFINED);
    }
    setRebalancerRef(RebalancerRef.from(FullAutoRebalancer.class));
  }

  /**
   * Builder for a full auto rebalancer config. By default, it corresponds to
   * {@link FullAutoRebalancer}
   */
  public static final class Builder extends PartitionedRebalancerConfig.AbstractBuilder<Builder> {
    /**
     * Instantiate with a resource
     * @param resourceId resource id
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
      super.rebalancerRef(RebalancerRef.from(FullAutoRebalancer.class));
      super.rebalanceMode(RebalanceMode.FULL_AUTO);
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public FullAutoRebalancerConfig build() {
      FullAutoRebalancerConfig config = new FullAutoRebalancerConfig();
      super.update(config);
      return config;
    }
  }
}
