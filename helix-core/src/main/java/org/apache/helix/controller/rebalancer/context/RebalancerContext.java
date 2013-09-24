package org.apache.helix.controller.rebalancer.context;

import java.util.Map;
import java.util.Set;

import org.apache.helix.api.Partition;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.StateModelDefId;
import org.apache.helix.api.StateModelFactoryId;

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
 * Defines the state available to a rebalancer. The most common use case is to use a
 * {@link PartitionedRebalancerContext} or a subclass and set up a resource with it. A rebalancer
 * configuration, at a minimum, is aware of subunits of a resource, the state model to follow, and
 * how the configuration should be serialized.
 */
public interface RebalancerContext {
  /**
   * Get a map of resource partition identifiers to partitions. A partition is a subunit of a
   * resource, e.g. a subtask of a task
   * @return map of (subunit id, subunit) pairs
   */
  public Map<? extends PartitionId, ? extends Partition> getSubUnitMap();

  /**
   * Get the subunits of the resource (e.g. partitions)
   * @return set of subunit ids
   */
  public Set<? extends PartitionId> getSubUnitIdSet();

  /**
   * Get a specific subunit
   * @param subUnitId the id of the subunit
   * @return SubUnit
   */
  public Partition getSubUnit(PartitionId subUnitId);

  /**
   * Get the resource to rebalance
   * @return resource id
   */
  public ResourceId getResourceId();

  /**
   * Get the state model definition that the resource follows
   * @return state model definition id
   */
  public StateModelDefId getStateModelDefId();

  /**
   * Get the state model factory of this resource
   * @return state model factory id
   */
  public StateModelFactoryId getStateModelFactoryId();

  /**
   * Get the tag, if any, that participants must have in order to serve this resource
   * @return participant group tag, or null
   */
  public String getParticipantGroupTag();

  /**
   * Get the serializer for this context
   * @return ContextSerializer class object
   */
  public Class<? extends ContextSerializer> getSerializerClass();

  /**
   * Get a reference to the class used to rebalance this resource
   * @return RebalancerRef
   */
  public RebalancerRef getRebalancerRef();
}
