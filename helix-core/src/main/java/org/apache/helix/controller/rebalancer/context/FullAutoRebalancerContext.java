package org.apache.helix.controller.rebalancer.context;

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
 * RebalancerContext for FULL_AUTO rebalancing mode. By default, it corresponds to
 * {@link FullAutoRebalancer}
 */
public class FullAutoRebalancerContext extends PartitionedRebalancerContext {
  public FullAutoRebalancerContext() {
    super(RebalanceMode.FULL_AUTO);
    setRebalancerRef(RebalancerRef.from(FullAutoRebalancer.class));
  }

  /**
   * Builder for a full auto rebalancer context. By default, it corresponds to
   * {@link FullAutoRebalancer}
   */
  public static final class Builder extends PartitionedRebalancerContext.AbstractBuilder<Builder> {
    /**
     * Instantiate with a resource
     * @param resourceId resource id
     */
    public Builder(ResourceId resourceId) {
      super(resourceId);
      super.rebalancerRef(RebalancerRef.from(FullAutoRebalancer.class));
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public FullAutoRebalancerContext build() {
      FullAutoRebalancerContext context = new FullAutoRebalancerContext();
      super.update(context);
      return context;
    }
  }
}
