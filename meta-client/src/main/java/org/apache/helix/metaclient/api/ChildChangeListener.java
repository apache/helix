package org.apache.helix.metaclient.api;

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
 * Listener interface for children change events on a particular key. It includes new child
 * creation, child deletion, child data change.
 * TODO: add type for persist listener is removed
 * For hierarchy key spaces like zookeeper, it refers to an entry's entire subtree.
 * For flat key spaces, it refers to keys that matches `prefix*`.
 */
public interface ChildChangeListener {
  enum ChangeType {
    ENTRY_CREATED,     // Any child entry created
    ENTRY_DELETED,     // Any child entry deleted
    ENTRY_DATA_CHANGE  // Any child entry has value change
  }
  /**
   * Called when any child of the current key has changed.
   */
  void handleChildChange(String changedPath, ChangeType changeType) throws Exception;
}