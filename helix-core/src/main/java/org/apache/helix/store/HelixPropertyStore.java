package org.apache.helix.store;

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

import org.apache.helix.BaseDataAccessor;

public interface HelixPropertyStore<T> extends BaseDataAccessor<T> {
  /**
   * Perform resource allocation when property store starts
   * Resource allocation includes: - start an internal thread for fire callbacks
   */
  public void start();

  /**
   * Perform clean up when property store stops
   * Cleanup includes: - stop the internal thread for fire callbacks
   */
  public void stop();

  /**
   * Register a listener to a parent path.
   * Subscribing to a parent path means any changes happening under the parent path will
   * notify the listener
   * @param parentPath
   * @param listener
   */
  public void subscribe(String parentPath, HelixPropertyListener listener);

  /**
   * Remove a listener from a parent path.
   * This will remove the listener from receiving any notifications happening under the
   * parent path
   * @param parentPath
   * @param listener
   */
  public void unsubscribe(String parentPath, HelixPropertyListener listener);
}
