package org.apache.helix.model.builder;

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

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.api.id.StateModelFactoryId;
import org.apache.helix.model.IdealState;

public abstract class IdealStateBuilder {
  /**
   * Number of partitions/subresources
   */
  private int numPartitions;
  /**
   * Number of replicas for each partition
   */
  private int numReplica;
  /**
   * State model that is applicable for this resource
   */
  private String stateModel;
  /**
   * The state model factory implementation in the participant. Allows
   * participants to plugin resource specific implementation, by default Helix
   * uses the implementation specified per state model.<br/>
   * This is optional
   */
  private String stateModelFactoryName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY;
  /**
   * Helix rebalancer strategies. AUTO, SEMI_AUTO, CUSTOMIZED
   */
  protected IdealState.RebalanceMode rebalancerMode;
  /**
   * A constraint that limits the maximum number of partitions per Node.
   */
  private int maxPartitionsPerNode;
  /**
   * Allocate the resource to nodes that are tagged with a specific "nodeGroup"
   * name. By default a resource will be allocated to all nodes registered to
   * the cluster.
   */
  private String nodeGroup;

  protected ZNRecord _record;

  /**
   * @param resourceName
   */
  public IdealStateBuilder(String resourceName) {
    _record = new ZNRecord(resourceName);
  }

  /**
   * Instantiate with a resource id
   * @param resourceId the resource for which to build an ideal state
   */
  public IdealStateBuilder(ResourceId resourceId) {
    this(resourceId.stringify());
  }

  /**
   * @param numReplica
   */
  public IdealStateBuilder setNumReplica(int numReplica) {
    this.numReplica = numReplica;
    return this;
  }

  /**
   * @param numPartitions
   */
  public IdealStateBuilder setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
    return this;
  }

  /**
   * @param stateModel
   */
  public IdealStateBuilder setStateModel(String stateModel) {
    this.stateModel = stateModel;
    return this;
  }

  /**
   * Set the state model definition to use with this ideal state
   * @param stateModelDefId state model identifier
   */
  public IdealStateBuilder setStateModelDefId(StateModelDefId stateModelDefId) {
    this.stateModel = stateModelDefId.stringify();
    return this;
  }

  /**
   * @param stateModelFactoryName
   */
  public IdealStateBuilder setStateModelFactoryName(String stateModelFactoryName) {
    this.stateModelFactoryName = stateModelFactoryName;
    return this;
  }

  /**
   * @param maxPartitionsPerNode
   */
  public IdealStateBuilder setMaxPartitionsPerNode(int maxPartitionsPerNode) {
    this.maxPartitionsPerNode = maxPartitionsPerNode;
    return this;
  }

  /**
   * @param nodeGroup
   */
  public IdealStateBuilder setNodeGroup(String nodeGroup) {
    this.nodeGroup = nodeGroup;
    return this;
  }

  /**
   * sub-class should implement this to set ideal-state mode
   * @return
   */
  public IdealStateBuilder setRebalancerMode(IdealState.RebalanceMode rebalancerMode) {
    this.rebalancerMode = rebalancerMode;
    return this;
  }

  /**
   * @return
   */
  public IdealState build() {
    IdealState idealstate = new IdealState(_record);
    idealstate.setNumPartitions(numPartitions);
    idealstate.setMaxPartitionsPerInstance(maxPartitionsPerNode);
    idealstate.setStateModelDefId(StateModelDefId.from(stateModel));
    idealstate.setStateModelFactoryId(StateModelFactoryId.from(stateModelFactoryName));
    idealstate.setRebalanceMode(rebalancerMode);
    idealstate.setReplicas("" + numReplica);
    if (nodeGroup != null) {
      idealstate.setInstanceGroupTag(nodeGroup);
    }

    if (!idealstate.isValid()) {
      throw new HelixException("invalid ideal-state: " + idealstate);
    }
    return idealstate;
  }
}
