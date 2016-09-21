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
import org.apache.helix.model.IdealState;

public abstract class IdealStateBuilder {
  /**
   * Resource name e.g. myDB,
   */
  private String resourceName;
  /**
   * Number of partitions/subresources
   */
  private int numPartitions;

  /**
   * Number of replicas for each partition
   */
  private int numReplica;


  /**
   * Number of minimal active replicas for each partition
   */
  private int minActiveReplica = -1;

  /**
   * The delay time (in ms) that Helix should move the partition after an instance goes offline.
   */
  private long rebalanceDelayInMs = -1;

  /**
   * Whether delay rebalance should be disabled.
   */
  private Boolean delayRebalanceDisabled = null;

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
   * Customized rebalancer class.
   */
  private String rebalancerClassName;

  /**
   * Custom rebalance strategy
   */
  private String rebalanceStrategy;

  /**
   * A constraint that limits the maximum number of partitions per Node.
   */
  private int maxPartitionsPerNode;
  /**
   * Allocate the resource to nodes that are tagged with a specific "nodeGroup"
   * name. By default a resource will be allocated to all nodes registered to
   * the cluster.
   */
  private String nodeGroup = "*";

  /**
   * Whether to disable external view for this resource
   */
  private Boolean disableExternalView = null;

  /**
   * Resource Group Name
   */
  private String resourceGroupName;

  /**
   * Whether enabling group routing.
   */
  private Boolean enableGroupRouting = null;


  /**
   * Resource Type
   */
  private String resourceType;


  protected ZNRecord _record;

  /**
   * @param resourceName
   */
  public IdealStateBuilder(String resourceName) {
    this.resourceName = resourceName;
    _record = new ZNRecord(resourceName);
  }

  /**
   * @param numReplica
   */
  public IdealStateBuilder setNumReplica(int numReplica) {
    this.numReplica = numReplica;
    return this;
  }

  /**
   * @param minActiveReplica
   * @return
   */
  public IdealStateBuilder setMinActiveReplica(int minActiveReplica) {
    this.minActiveReplica = minActiveReplica;
    return this;
  }

  public IdealStateBuilder setRebalanceDelay(int delayInMilliseconds) {
    this.rebalanceDelayInMs = delayInMilliseconds;
    return this;
  }

  /**
   * Disable Delayed Rebalance.
   */
  public void disableDelayRebalance() {
    delayRebalanceDisabled = true;
  }

  /**
   * Disable Delayed Rebalance.
   */
  public void enableDelayRebalance() {
    delayRebalanceDisabled = false;
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
   * Disable ExternalView for this resource
   */
  public IdealStateBuilder disableExternalView() {
    this.disableExternalView = true;
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
   * Set custom rebalancer class name.
   * @return IdealStateBuilder
   */
  public IdealStateBuilder setRebalancerClass(String rebalancerClassName) {
    this.rebalancerClassName = rebalancerClassName;
    return this;
  }

  /**
   * Set custom rebalance strategy name.
   * @param rebalanceStrategy
   * @return
   */
  public IdealStateBuilder setRebalanceStrategy(String rebalanceStrategy) {
    this.rebalanceStrategy = rebalanceStrategy;
    return this;
  }

  /**
   *
   * @param resourceGroupName
   * @return
   */
  public IdealStateBuilder setResourceGroupName(String resourceGroupName) {
    this.resourceGroupName = resourceGroupName;
    return this;
  }

  /**
   * @param resourceType
   * @return
   */
  public IdealStateBuilder setResourceType(String resourceType) {
    this.resourceType = resourceType;
    return this;
  }

  /**
   * Enable Group Routing for this resource.
   * @return
   */
  public IdealStateBuilder enableGroupRouting() {
    this.enableGroupRouting = true;
    return this;
  }

  /**
   * @return
   */
  public IdealState build() {
    IdealState idealstate = new IdealState(_record);
    idealstate.setNumPartitions(numPartitions);
    idealstate.setStateModelDefRef(stateModel);
    idealstate.setStateModelFactoryName(stateModelFactoryName);
    idealstate.setRebalanceMode(rebalancerMode);
    idealstate.setReplicas("" + numReplica);

    if (minActiveReplica >= 0) {
      idealstate.setMinActiveReplicas(minActiveReplica);
    }

    if (rebalancerClassName != null) {
      idealstate.setRebalancerClassName(rebalancerClassName);
    }

    if (rebalanceStrategy != null) {
      idealstate.setRebalanceStrategy(rebalanceStrategy);
    }

    if (maxPartitionsPerNode > 0) {
      idealstate.setMaxPartitionsPerInstance(maxPartitionsPerNode);
    }

    if (disableExternalView != null) {
      idealstate.setDisableExternalView(disableExternalView);
    }

    if (enableGroupRouting != null) {
      idealstate.enableGroupRouting(enableGroupRouting);
    }

    if (resourceGroupName != null) {
      idealstate.setResourceGroupName(resourceGroupName);
    }

    if (resourceType != null) {
      idealstate.setResourceType(resourceType);
    }

    if (rebalanceDelayInMs >= 0) {
      idealstate.setRebalanceDelay(rebalanceDelayInMs);
    }

    if (delayRebalanceDisabled != null) {
      idealstate.setDelayRebalanceDisabled(delayRebalanceDisabled);
    }

    if (!idealstate.isValid()) {
      throw new HelixException("invalid ideal-state: " + idealstate);
    }
    return idealstate;
  }

}
