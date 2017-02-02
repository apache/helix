package org.apache.helix;

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
 * Describes various properties that operations involving {@link Message} delivery will follow.
 */
public class Criteria {
  public enum DataSource {
    IDEALSTATES,
    EXTERNALVIEW
  }

  /**
   * This can be CONTROLLER, PARTICIPANT, ROUTER Cannot be null
   */
  InstanceType recipientInstanceType;
  /**
   * If true this will only be process by the instance that was running when the
   * message was sent. If the instance process dies and comes back up it will be
   * ignored.
   */
  boolean sessionSpecific;
  /**
   * applicable only in case PARTICIPANT use % to broadcast to all instances
   */
  String instanceName = "";
  /**
   * Name of the resource. Use % to send message to all resources
   * owned by an instance.
   */
  String resourceName = "";
  /**
   * Resource partition. Use % to send message to all partitions of a given
   * resource
   */
  String partitionName = "";
  /**
   * State of the resource
   */
  String partitionState = "";
  /**
   * Exclude sending message to your self. True by default
   */
  boolean selfExcluded = true;
  /**
   * Determine if use external view or ideal state as source of truth
   */
  DataSource _dataSource = DataSource.EXTERNALVIEW;

  /**
   * Get the current source of truth
   * @return either the ideal state or the external view
   */
  public DataSource getDataSource() {
    return _dataSource;
  }

  /**
   * Set the current source of truth
   * @param source ideal state or external view
   */
  public void setDataSource(DataSource source) {
    _dataSource = source;
  }

  /**
   * Determine if the message is excluded from being sent to the sender
   * @return true if the self-sent message is excluded, false otherwise
   */
  public boolean isSelfExcluded() {
    return selfExcluded;
  }

  /**
   * Indicate whether or not the sender will be excluded as a message recipient
   * @param selfExcluded true if the sender should be excluded, false otherwise
   */
  public void setSelfExcluded(boolean selfExcluded) {
    this.selfExcluded = selfExcluded;
  }

  /**
   * Determine the type of the recipient
   * @return InstanceType (e.g. PARTICIPANT, CONTROLLER, SPECTATOR)
   */
  public InstanceType getRecipientInstanceType() {
    return recipientInstanceType;
  }

  /**
   * Set the type of the recipient
   * @param recipientInstanceType InstanceType (e.g. PARTICIPANT, CONTROLLER, SPECTATOR)
   */
  public void setRecipientInstanceType(InstanceType recipientInstanceType) {
    this.recipientInstanceType = recipientInstanceType;
  }

  /**
   * Determine if this message should be processed only if an instance was up at send time
   * @return true if the message will be processed by current live nodes, false otherwise
   */
  public boolean isSessionSpecific() {
    return sessionSpecific;
  }

  /**
   * Indicate whether or not a message should be restricted to a session
   * @param sessionSpecific true if the message can only be processed by live nodes at send time,
   *          false otherwise
   */
  public void setSessionSpecific(boolean sessionSpecific) {
    this.sessionSpecific = sessionSpecific;
  }

  /**
   * Get the name of the destination instance, available only for PARTICIPANT
   * @return the instance name
   */
  public String getInstanceName() {
    return instanceName;
  }

  /**
   * Set the name of the destination instance (PARTICIPANT only)
   * @param instanceName the instance name or % for all instances
   */
  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }

  /**
   * Get the destination resource name
   * @return destination resource name
   */
  public String getResource() {
    return resourceName;
  }

  /**
   * Set the destination resource name
   * @param resourceName the resource name or % for all resources
   */
  public void setResource(String resourceName) {
    this.resourceName = resourceName;
  }

  /**
   * Get the destination partition name
   * @return destination partition name
   */
  public String getPartition() {
    return partitionName;
  }

  /**
   * Set the destination partition name
   * @param partitionName the partition name, or % for all partitions of a resource
   */
  public void setPartition(String partitionName) {
    this.partitionName = partitionName;
  }

  /**
   * Get the state of a resource partition
   * @return the state of the resource partition
   */
  public String getPartitionState() {
    return partitionState;
  }

  /**
   * Set the state of the resource partition
   * @param partitionState the state of the resource partition
   */
  public void setPartitionState(String partitionState) {
    this.partitionState = partitionState;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("instanceName").append("=").append(instanceName);
    sb.append("resourceName").append("=").append(resourceName);
    sb.append("partitionName").append("=").append(partitionName);
    sb.append("partitionState").append("=").append(partitionState);
    return sb.toString();
  }

}
