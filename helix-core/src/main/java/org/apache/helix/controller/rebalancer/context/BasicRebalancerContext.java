package org.apache.helix.controller.rebalancer.context;

import java.util.Set;

import org.apache.helix.api.Partition;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.StateModelDefId;
import org.apache.helix.api.StateModelFactoryId;
import org.codehaus.jackson.annotate.JsonIgnore;

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
 * Abstract RebalancerContext that functions for generic subunits. Use a subclass that more
 * concretely defines the subunits.
 */
public abstract class BasicRebalancerContext implements RebalancerContext {
  private ResourceId _resourceId;
  private StateModelDefId _stateModelDefId;
  private StateModelFactoryId _stateModelFactoryId;
  private String _participantGroupTag;
  private Class<? extends ContextSerializer> _serializer;
  private RebalancerRef _rebalancerRef;

  /**
   * Instantiate a basic rebalancer context
   */
  public BasicRebalancerContext() {
    _serializer = DefaultContextSerializer.class;
  }

  @Override
  public ResourceId getResourceId() {
    return _resourceId;
  }

  /**
   * Set the resource to rebalance
   * @param resourceId resource id
   */
  public void setResourceId(ResourceId resourceId) {
    _resourceId = resourceId;
  }

  @Override
  public StateModelDefId getStateModelDefId() {
    return _stateModelDefId;
  }

  /**
   * Set the state model definition that the resource follows
   * @param stateModelDefId state model definition id
   */
  public void setStateModelDefId(StateModelDefId stateModelDefId) {
    _stateModelDefId = stateModelDefId;
  }

  @Override
  public StateModelFactoryId getStateModelFactoryId() {
    return _stateModelFactoryId;
  }

  /**
   * Set the state model factory that the resource uses
   * @param stateModelFactoryId state model factory id
   */
  public void setStateModelFactoryId(StateModelFactoryId stateModelFactoryId) {
    _stateModelFactoryId = stateModelFactoryId;
  }

  @Override
  public String getParticipantGroupTag() {
    return _participantGroupTag;
  }

  /**
   * Set a tag that participants must have in order to serve this resource
   * @param participantGroupTag string group tag
   */
  public void setParticipantGroupTag(String participantGroupTag) {
    _participantGroupTag = participantGroupTag;
  }

  /**
   * Get the serializer. If none is provided, {@link DefaultContextSerializer} is used
   */
  @Override
  public Class<? extends ContextSerializer> getSerializerClass() {
    return _serializer;
  }

  /**
   * Set the class that can serialize this context
   * @param serializer serializer class that implements ContextSerializer
   */
  public void setSerializerClass(Class<? extends ContextSerializer> serializer) {
    _serializer = serializer;
  }

  @Override
  @JsonIgnore
  public Set<? extends PartitionId> getSubUnitIdSet() {
    return getSubUnitMap().keySet();
  }

  @Override
  @JsonIgnore
  public Partition getSubUnit(PartitionId subUnitId) {
    return getSubUnitMap().get(subUnitId);
  }

  @Override
  public RebalancerRef getRebalancerRef() {
    return _rebalancerRef;
  }

  /**
   * Set the reference to the class used to rebalance this resource
   * @param rebalancerRef RebalancerRef instance
   */
  public void setRebalancerRef(RebalancerRef rebalancerRef) {
    _rebalancerRef = rebalancerRef;
  }

  /**
   * Abstract builder for the base rebalancer context
   */
  public static abstract class AbstractBuilder<T extends AbstractBuilder<T>> {
    private final ResourceId _resourceId;
    private StateModelDefId _stateModelDefId;
    private StateModelFactoryId _stateModelFactoryId;
    private String _participantGroupTag;
    private Class<? extends ContextSerializer> _serializerClass;
    private RebalancerRef _rebalancerRef;

    /**
     * Instantiate with a resource id
     * @param resourceId resource id
     */
    public AbstractBuilder(ResourceId resourceId) {
      _resourceId = resourceId;
      _serializerClass = DefaultContextSerializer.class;
    }

    /**
     * Set the state model definition that the resource should follow
     * @param stateModelDefId state model definition id
     * @return Builder
     */
    public T stateModelDefId(StateModelDefId stateModelDefId) {
      _stateModelDefId = stateModelDefId;
      return self();
    }

    /**
     * Set the state model factory that the resource should use
     * @param stateModelFactoryId state model factory id
     * @return Builder
     */
    public T stateModelFactoryId(StateModelFactoryId stateModelFactoryId) {
      _stateModelFactoryId = stateModelFactoryId;
      return self();
    }

    /**
     * Set the tag that all participants require in order to serve this resource
     * @param participantGroupTag the tag
     * @return Builder
     */
    public T participantGroupTag(String participantGroupTag) {
      _participantGroupTag = participantGroupTag;
      return self();
    }

    /**
     * Set the serializer class for this rebalancer context
     * @param serializerClass class that implements ContextSerializer
     * @return Builder
     */
    public T serializerClass(Class<? extends ContextSerializer> serializerClass) {
      _serializerClass = serializerClass;
      return self();
    }

    /**
     * Specify a custom class to use for rebalancing
     * @param rebalancerRef RebalancerRef instance
     * @return Builder
     */
    public T rebalancerRef(RebalancerRef rebalancerRef) {
      _rebalancerRef = rebalancerRef;
      return self();
    }

    /**
     * Update an existing context with base fields
     * @param context derived context
     */
    protected final void update(BasicRebalancerContext context) {
      context.setResourceId(_resourceId);
      context.setStateModelDefId(_stateModelDefId);
      context.setStateModelFactoryId(_stateModelFactoryId);
      context.setParticipantGroupTag(_participantGroupTag);
      context.setSerializerClass(_serializerClass);
      context.setRebalancerRef(_rebalancerRef);
    }

    /**
     * Get a typed reference to "this" class. Final derived classes should simply return the this
     * reference.
     * @return this for the most specific type
     */
    protected abstract T self();

    /**
     * Get the rebalancer context from the built fields
     * @return RebalancerContext
     */
    public abstract RebalancerContext build();
  }
}
