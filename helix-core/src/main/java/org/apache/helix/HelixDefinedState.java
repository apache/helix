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
 * helix defined states
 * ERROR : when errors happen during state transitions, transit to ERROR state
 * participant will also invoke state-model.on-err(), ignore errors in state-model.on-err()
 * when drop resource in ERROR state and not disabled, controller sends ERROR->DROPPED transition
 * if errors happen in ERROR->DROPPED transition, participant will disable resource/partition
 * when disable resource/partition in ERROR state, resource/partition will be marked disabled
 * but controller not send any transitions
 * when reset resource/partition in ERROR state and not disabled
 * controller send ERROR->initial-state transition
 * if errors happen in ERROR->initial-state transition, remain in ERROR state
 * DROPPED : when drop resource in a non-ERROR state and not disabled
 * controller sends all the transitions from current-state to initial-state
 * then sends initial-state->DROPPED transition
 * @see HELIX-43: add support for dropping partitions in error state
 */
public enum HelixDefinedState {
  ERROR,
  DROPPED
}
