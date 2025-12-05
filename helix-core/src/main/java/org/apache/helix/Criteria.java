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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Specifies recipient criteria for message delivery in a Helix cluster.
 * 
 * <p>The {@link Criteria} object defines which instances should receive a message by specifying
 * attributes like instance name, resource, partition, and state. The most critical configuration
 * is {@link DataSource}, which determines where Helix looks up cluster state to resolve recipients.
 * 
 * <p><b>PERFORMANCE WARNING:</b> Using {@link DataSource#EXTERNALVIEW} with wildcard or unspecified
 * resource names causes Helix to scan ALL ExternalView znodes in the cluster, regardless of other
 * criteria fields. At scale (thousands of resources), this causes severe performance degradation.
 * 
 * <p><b>Quick Start - Common Patterns:</b>
 * <pre>
 * // Pattern 1: Send to specific live instance (most efficient)
 * Criteria criteria = new Criteria();
 * criteria.setInstanceName("host_1234");
 * criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
 * criteria.setDataSource(DataSource.LIVEINSTANCES);
 * criteria.setSessionSpecific(true);
 * 
 * // Pattern 2: Send to all replicas of a specific partition
 * Criteria criteria = new Criteria();
 * criteria.setInstanceName("%");
 * criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
 * criteria.setDataSource(DataSource.EXTERNALVIEW);
 * criteria.setResource("MyDatabase");  // IMPORTANT: Specify exact resource name
 * criteria.setPartition("MyDatabase_5");
 * criteria.setSessionSpecific(true);
 * 
 * // Pattern 3: Broadcast to all live instances
 * Criteria criteria = new Criteria();
 * criteria.setInstanceName("%");
 * criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
 * criteria.setDataSource(DataSource.LIVEINSTANCES);
 * criteria.setSessionSpecific(true);
 * </pre>
 * 
 * <p><b>DataSource Selection Guide:</b>
 * <ul>
 *   <li><b>LIVEINSTANCES:</b> Use when targeting live instances without resource/partition filtering.
 *       Fastest option - reads only LIVEINSTANCES znodes.</li>
 *   <li><b>EXTERNALVIEW:</b> Use when filtering by resource, partition, or replica state.
 *       ALWAYS specify exact resource names to avoid scanning all ExternalViews.</li>
 *   <li><b>INSTANCES:</b> Use when targeting all configured instances (live or not) based on
 *       instance configuration.</li>
 *   <li><b>IDEALSTATES:</b> Use when targeting based on ideal state configuration rather than
 *       current state. Less common.</li>
 * </ul>
 * 
 * @see ClusterMessagingService#send(Criteria, org.apache.helix.model.Message)
 * @see org.apache.helix.messaging.CriteriaEvaluator
 */
public class Criteria {
  /**
   * Specifies the source of cluster state information for resolving message recipients.
   * 
   * <p>The DataSource determines which ZooKeeper znodes Helix reads to match the criteria:
   * <ul>
   *   <li><b>LIVEINSTANCES:</b> Reads /LIVEINSTANCES znodes. Contains only currently connected
   *       instances. Use when you don't need resource/partition information. Fastest option.</li>
   *   <li><b>INSTANCES:</b> Reads /INSTANCES/[instance] znodes. Contains instance configuration
   *       (host, port, enabled/disabled). Use for targeting based on instance config.</li>
   *   <li><b>EXTERNALVIEW:</b> Reads /EXTERNALVIEWS/[resource] znodes. Contains actual current
   *       replica placement and states (MASTER/SLAVE/OFFLINE). Use when you need resource/partition/state
   *       filtering. <b>WARNING:</b> Wildcard resource names scan ALL ExternalViews.</li>
   *   <li><b>IDEALSTATES:</b> Reads /IDEALSTATES/[resource] znodes. Contains desired replica
   *       placement. Similar performance to EXTERNALVIEW but less commonly used.</li>
   * </ul>
   * 
   * <p><b>Performance Impact:</b> LIVEINSTANCES is fastest as it reads minimal data. EXTERNALVIEW
   * and IDEALSTATES can be slow at scale if wildcards are used in resource names, as Helix must
   * read and deserialize all resource znodes to match the criteria.
   */
  public enum DataSource {
    IDEALSTATES,
    EXTERNALVIEW,
    LIVEINSTANCES,
    INSTANCES
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
   * The name of target cluster. If null, means sending to the local cluster
   */
  String _clusterName = null;

  /**
   * Get the current source of truth
   * @return either the ideal state or the external view
   */
  public DataSource getDataSource() {
    return _dataSource;
  }

  /**
   * Set the current source of truth for resolving message recipients.
   * 
   * <p><b>PERFORMANCE GUIDANCE:</b>
   * <ul>
   *   <li>Use {@link DataSource#LIVEINSTANCES} when you only need to target live instances
   *       and don't require resource/partition/state filtering.</li>
   *   <li>If using {@link DataSource#EXTERNALVIEW}, always specify exact resource names via
   *       {@link #setResource(String)} to avoid scanning all ExternalView znodes.</li>
   * </ul>
   * 
   * @param source ideal state, external view, live instances, or instances
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
   * Set the destination resource name.
   * 
   * <p><b>Note:</b> This field is only meaningful when using {@link DataSource#EXTERNALVIEW} or
   * {@link DataSource#IDEALSTATES}. It is ignored for LIVEINSTANCES and INSTANCES.
   * 
   * <p><b>PERFORMANCE:</b> When using EXTERNALVIEW, specifying an exact resource name (e.g., "MyDatabase")
   * reads only that resource's ExternalView znode. Using wildcard "%" reads ALL ExternalView znodes
   * in the cluster, which can cause severe performance issues at scale.
   * 
   * @param resourceName the exact resource name, or "%" for all resources (avoid wildcard at scale)
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

  /**
   * Get the target cluster name
   * @return the target cluster name if set or null if not set
   */
  public String getClusterName() {
    return _clusterName;
  }

  /**
   * Set the target cluster name
   * @param clusterName target cluster name to send message
   */
  public void setClusterName(String clusterName) {
    _clusterName = clusterName;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("instanceName").append("=").append(instanceName);
    sb.append("resourceName").append("=").append(resourceName);
    sb.append("partitionName").append("=").append(partitionName);
    sb.append("partitionState").append("=").append(partitionState);
    if (_clusterName != null) {
      sb.append("clusterName").append("=").append(_clusterName);
    }
    return sb.toString();
  }
}
