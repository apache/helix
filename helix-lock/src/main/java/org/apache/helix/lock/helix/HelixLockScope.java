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

package org.apache.helix.lock.helix;

import java.util.List;

import org.apache.helix.lock.LockScope;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.util.StringTemplate;


/**
 *  Defines the various scopes of Helix locks, and how they are represented on Zookeeper
 */
public class HelixLockScope implements LockScope {

  /**
   * Define various properties of Helix lock, and associate them with the number of arguments required for getting znode path
   */
  public enum LockScopeProperty {

    CLUSTER(2),

    PARTICIPANT(2),

    RESOURCE(2);

    //the number of arguments required to generate a full path for the specific scope
    final int _pathArgNum;

    /**
     * Initialize a LockScopeProperty
     * @param pathArgNum the number of arguments required to generate a full path for the specific scope
    \     */
    private LockScopeProperty(int pathArgNum) {
      _pathArgNum = pathArgNum;
    }

    /**
     * Get the number of template arguments required to generate a full path
     * @return number of template arguments in the path
     */
    public int getPathArgNum() {
      return _pathArgNum;
    }
  }

  /**
   * string templates to generate znode path
   */
  private static final StringTemplate template = new StringTemplate();

  //TODO: Enrich the logic of path generation once we have a more detailed design
  static {
    template.addEntry(LockScopeProperty.CLUSTER, 2, "/{clusterName}/LOCK/CLUSTER/{clusterName}");
    template.addEntry(HelixLockScope.LockScopeProperty.PARTICIPANT, 2,
        "/{clusterName}/LOCK/PARTICIPANT/{participantName}");
    template.addEntry(HelixLockScope.LockScopeProperty.RESOURCE, 2,
        "/{clusterName}/LOCK/RESOURCE/{resourceName}");
  }

  private final String _path;

  /**
   * Initialize with a type of scope and unique identifiers
   * @param type the scope
   * @param pathKeys keys identifying a ZNode location
   */
  public HelixLockScope(HelixLockScope.LockScopeProperty type, List<String> pathKeys) {

    if (pathKeys.size() != type.getPathArgNum()) {
      throw new IllegalArgumentException(
          type + " requires " + type.getPathArgNum() + " arguments to get znode, but was: "
              + pathKeys);
    }
    _path = template.instantiate(type, pathKeys.toArray(new String[0]));
  }

  @Override
  public String getPath() {
    return _path;
  }
}
