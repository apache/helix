package org.apache.helix.api;

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

import org.apache.helix.api.id.ControllerId;
import org.apache.helix.model.LiveInstance;

/**
 * A helix controller
 */
public class Controller {
  private final ControllerId _id;
  private final LiveInstance _liveInstance;
  private final boolean _isLeader;

  /**
   * Construct a controller
   * @param id
   */
  public Controller(ControllerId id, LiveInstance liveInstance, boolean isLeader) {
    _id = id;
    _liveInstance = liveInstance;
    _isLeader = isLeader;
  }

  /**
   * Get controller id
   * @return controller id
   */
  public ControllerId getId() {
    return _id;
  }

  /**
   * Check if the controller is leader
   * @return true if leader or false otherwise
   */
  public boolean isLeader() {
    return _isLeader;
  }

  /**
   * Get the running instance
   * @return running instance or null if not running
   */
  public LiveInstance getLiveInstance() {
    return _liveInstance;
  }
}
