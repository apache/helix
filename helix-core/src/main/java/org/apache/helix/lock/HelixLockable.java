package org.apache.helix.lock;

import org.apache.helix.api.Scope;
import org.apache.helix.api.id.ClusterId;

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
 * Implemented by any Helix construct that is lockable and is able to return a HelixLock instance
 */
public interface HelixLockable {
  /**
   * Get a lock object on a scope
   * @param clusterId cluster to lock
   * @param scope scope relative to the cluster that the lock protects
   * @return HelixLock instance
   */
  HelixLock getLock(ClusterId clusterId, Scope<?> scope);
}
