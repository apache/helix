package org.apache.helix.controller.rebalancer.waged;

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

import org.apache.helix.model.IdealState;

import java.util.HashMap;
import java.util.Map;

/**
 * A placeholder before we have the real assignment metadata store.
 */
public class AssignmentMetadataStore {
  private Map<String, IdealState> _persistGlobalBaseline = new HashMap<>();
  private Map<String, IdealState> _persistBestPossibleAssignment = new HashMap<>();

  public Map<String, IdealState> getBaseline() {
    return _persistGlobalBaseline;
  }

  public void persistBaseline(Map<String, IdealState> globalBaseline) {
    // TODO clean up invalid items
    _persistGlobalBaseline = globalBaseline;
  }

  public Map<String, IdealState> getBestPossibleAssignment() {
    return _persistBestPossibleAssignment;
  }

  public void persistBestPossibleAssignment(Map<String, IdealState> bestPossibleAssignment) {
    // TODO clean up invalid items
    _persistBestPossibleAssignment.putAll(bestPossibleAssignment);
  }
}
