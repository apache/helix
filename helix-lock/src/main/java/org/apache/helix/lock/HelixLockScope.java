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

package org.apache.helix.lock;

import java.util.List;

import org.apache.helix.util.StringTemplate;


/**
 *  Defines the various scopes of Helix locks, and how they are represented on Zookeeper
 */
public class HelixLockScope {

  /**
   * Define various properties of Helix lock, and associate them with the number of arguments required for getting znode path
   */
  public enum LockScopeProperty {

    CLUSTER(1),

    PARTICIPANT(2),

    RESOURCE(3),

    PARTITION(4);

    //the number of arguments required to generate a full path for the specific scope
    final int _zkPathArgNum;

    /**
     * Initialize a LockScopeProperty
     * @param zkPathArgNum the number of arguments required to generate a full path for the specific scope
\     */
    private LockScopeProperty(int zkPathArgNum) {
      _zkPathArgNum = zkPathArgNum;
    }

    /**
     * Get the number of template arguments required to generate a full path
     * @return number of template arguments in the path
     */
    public int getZkPathArgNum() {
      return _zkPathArgNum;
    }

    /**
     * Get the position of this argument from the input that used to generate the scope
     * @return the number of position of value for this property in the list of keys input
     */
    public int getArgumentPos() {
      return _zkPathArgNum - 1;
    }
  }

  /**
   * string templates to generate znode path
   */
  private static final StringTemplate template = new StringTemplate();

  static {
    template.addEntry(LockScopeProperty.CLUSTER, 1, "/{clusterName}/LOCK");
    template.addEntry(HelixLockScope.LockScopeProperty.PARTICIPANT, 2,
        "/{clusterName}/LOCK/{participantName}");
    template.addEntry(HelixLockScope.LockScopeProperty.RESOURCE, 3,
        "/{clusterName}/LOCK/{participantName}/{resourceName}");
    template.addEntry(HelixLockScope.LockScopeProperty.PARTITION, 4,
        "/{clusterName}/LOCK/{participantName}/{resourceName}/{partitionName}");
  }

  private final HelixLockScope.LockScopeProperty _type;
  private final String _clusterName;
  private final String _participantName;
  private final String _resourceName;
  private final String _partitionName;

  private final String _zkPath;

  /**
   * Initialize with a type of scope and unique identifiers
   * @param type the scope
   * @param zkPathKeys keys identifying a ZNode location
   */
  public HelixLockScope(HelixLockScope.LockScopeProperty type, List<String> zkPathKeys) {

    if (zkPathKeys.size() != type.getZkPathArgNum()) {
      throw new IllegalArgumentException(
          type + " requires " + type.getZkPathArgNum() + " arguments to get znode, but was: "
              + zkPathKeys);
    }

    _type = type;

    //Initialize the name fields for various scope
    _clusterName = zkPathKeys.get(LockScopeProperty.CLUSTER.getArgumentPos());

    if (type.getZkPathArgNum() >= LockScopeProperty.PARTICIPANT.getZkPathArgNum()) {
      _participantName = zkPathKeys.get(LockScopeProperty.PARTICIPANT.getArgumentPos());
    } else {
      _participantName = null;
    }

    if (type.getZkPathArgNum() >= LockScopeProperty.RESOURCE.getZkPathArgNum()) {
      _resourceName = zkPathKeys.get(LockScopeProperty.RESOURCE.getArgumentPos());
    } else {
      _resourceName = null;
    }

    if (type.getZkPathArgNum() >= LockScopeProperty.PARTITION.getZkPathArgNum()) {
      _partitionName = zkPathKeys.get(LockScopeProperty.PARTITION.getArgumentPos());
    } else {
      _partitionName = null;
    }

    _zkPath = template.instantiate(type, zkPathKeys.toArray(new String[0])).toUpperCase();
  }

  /**
   * Get the scope
   * @return the type of scope
   */
  public HelixLockScope.LockScopeProperty getType() {
    return _type;
  }

  /**
   * Get the cluster name if it exists
   * @return the cluster name
   */
  public String getClusterName() {
    return _clusterName;
  }

  /**
   * Get the participant name if it exists
   * @return the participant name
   */
  public String getParticipantName() {
    return _participantName;
  }

  /**
   * Get the resource name if it exists
   * @return the resource name
   */
  public String getResourceName() {
    return _resourceName;
  }

  /**
   * Get the partition name if it exists
   * @return the partition name
   */
  public String getPartitionName() {
    return _partitionName;
  }

  /**
   * Get the path to the corresponding ZNode
   * @return a Zookeeper path
   */
  public String getZkPath() {
    return _zkPath;
  }
}
