package org.apache.helix.metaclient.recipes.leaderelection;

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
 * It provides APIs for listener listening on events like a new leader is created or current
 * leader node is deleted.
 */
public interface LeaderElectionListenerInterface {
  enum ChangeType {
    LEADER_ACQUIRED,
    LEADER_LOST
  }

  // When new leader is elected:
  //                          ChangeType == NEW_LEADER_ELECTED, curLeader is the new leader name
  // When no leader anymore:
  //                         ChangeType == LEADER_GONE, curLeader is an empty string
  // In ZK implementation, since notification does not include changed data and metaclient fetches
  // the entry when event comes, it is possible that
  public void onLeadershipChange(String leaderPath, ChangeType type,  String curLeader);
}