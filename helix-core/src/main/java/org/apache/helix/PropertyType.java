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
 * Types of nodes in a Helix cluster
 */
enum Type {
  CLUSTER,
  INSTANCE,
  CONTROLLER,
  RESOURCE;
}

/**
 * Types of data stored on Zookeeper by Helix
 */
public enum PropertyType {

  // @formatter:off
  // CLUSTER PROPERTIES
  CLUSTER(Type.CLUSTER, true, false, false, true, true),
  CONFIGS(Type.CLUSTER, true, false, false, false, true),
  LIVEINSTANCES(Type.CLUSTER, false, false, false, true, true),
  INSTANCES(Type.CLUSTER, true, false),
  IDEALSTATES(Type.CLUSTER, true, false, false, false, true),
  RESOURCEASSIGNMENTS(Type.CLUSTER, true, false),
  EXTERNALVIEW(Type.CLUSTER, true, false),
  STATEMODELDEFS(Type.CLUSTER, true, false, false, false, true),
  CONTROLLER(Type.CLUSTER, true, false),
  PROPERTYSTORE(Type.CLUSTER, true, false),

  // INSTANCE PROPERTIES
  MESSAGES(Type.INSTANCE, true, true, true),
  CURRENTSTATES(Type.INSTANCE, true, true, false, false, true),
  STATUSUPDATES(Type.INSTANCE, true, true, false, false, false, true),
  ERRORS(Type.INSTANCE, true, true),
  HEALTHREPORT(Type.INSTANCE, true, false, false, false, false, true),

  // CONTROLLER PROPERTY
  LEADER(Type.CONTROLLER, false, false, true, true),
  HISTORY(Type.CONTROLLER, true, true, true),
  PAUSE(Type.CONTROLLER, true, false, true),
  MESSAGES_CONTROLLER(Type.CONTROLLER, true, false, true),
  STATUSUPDATES_CONTROLLER(Type.CONTROLLER, true, true, true),
  ERRORS_CONTROLLER(Type.CONTROLLER, true, true, true),
  CONTEXT(Type.CONTROLLER, true, false);

  // @formatter:on

  Type type;

  boolean isPersistent;

  boolean mergeOnUpdate;

  boolean updateOnlyOnExists;

  boolean createOnlyIfAbsent;

  /**
   * "isCached" defines whether the property is cached in data accessor if data is cached,
   * then read from zk can be optimized
   */
  boolean isCached;

  private PropertyType(Type type, boolean isPersistent, boolean mergeOnUpdate) {
    this(type, isPersistent, mergeOnUpdate, false);
  }

  private PropertyType(Type type, boolean isPersistent, boolean mergeOnUpdate,
      boolean updateOnlyOnExists) {
    this(type, isPersistent, mergeOnUpdate, false, false);
  }

  private PropertyType(Type type, boolean isPersistent, boolean mergeOnUpdate,
      boolean updateOnlyOnExists, boolean createOnlyIfAbsent) {
    this(type, isPersistent, mergeOnUpdate, updateOnlyOnExists, createOnlyIfAbsent, false);
  }

  private PropertyType(Type type, boolean isPersistent, boolean mergeOnUpdate,
      boolean updateOnlyOnExists, boolean createOnlyIfAbsent, boolean isCached) {
    this(type, isPersistent, mergeOnUpdate, updateOnlyOnExists, createOnlyIfAbsent, isCached, false);
  }

  private PropertyType(Type type, boolean isPersistent, boolean mergeOnUpdate,
      boolean updateOnlyOnExists, boolean createOnlyIfAbsent, boolean isCached, boolean isAsyncWrite) {
    this.type = type;
    this.isPersistent = isPersistent;
    this.mergeOnUpdate = mergeOnUpdate;
    this.updateOnlyOnExists = updateOnlyOnExists;
    this.createOnlyIfAbsent = createOnlyIfAbsent;
    this.isCached = isCached;
  }

  /**
   * Determine if the property should only be created if it does not exist
   * @return true if it can only be created if absent, false otherwise
   */
  public boolean isCreateOnlyIfAbsent() {
    return createOnlyIfAbsent;
  }

  /**
   * Set policy for creating only if it does not already exist
   * @param createIfAbsent
   */
  public void setCreateIfAbsent(boolean createIfAbsent) {
    this.createOnlyIfAbsent = createIfAbsent;
  }

  /**
   * Gets the type of the associated node
   * @return {@link Type}
   */
  public Type getType() {
    return type;
  }

  /**
   * Set the type of the associated node
   * @param type {@link Type}
   */
  public void setType(Type type) {
    this.type = type;
  }

  /**
   * Get the persistent state of the property
   * @return true if persistent, false if ephemeral
   */
  public boolean isPersistent() {
    return isPersistent;
  }

  /**
   * Set the persistent state of the property
   * @param isPersistent
   */
  public void setPersistent(boolean isPersistent) {
    this.isPersistent = isPersistent;
  }

  /**
   * Determine if the property is merged or replaced on update
   * @return true if merge occurs on update, false otherwise
   */
  public boolean isMergeOnUpdate() {
    return mergeOnUpdate;
  }

  /**
   * Enable or disable merging on an update to this property
   * @param mergeOnUpdate
   */
  public void setMergeOnUpdate(boolean mergeOnUpdate) {
    this.mergeOnUpdate = mergeOnUpdate;
  }

  /**
   * Determine if this property is only updated if it exists
   * @return true if only updated when it exists, false otherwise
   */
  public boolean isUpdateOnlyOnExists() {
    return updateOnlyOnExists;
  }

  /**
   * Enable or disable updating only on existence
   * @param updateOnlyOnExists
   */
  public void setUpdateOnlyOnExists(boolean updateOnlyOnExists) {
    this.updateOnlyOnExists = updateOnlyOnExists;
  }

  /**
   * Check if value is cached
   * @return true if cached, false otherwise
   */
  public boolean isCached() {
    return isCached;
  }
}
