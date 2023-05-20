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

import org.apache.helix.metaclient.datamodel.DataRecord;


/**
 * This is the data represent leader election info of a leader election path.
 */
public class LeaderInfo {

  private String  _leaderElectionGroupName;
  private final DataRecord _record;


  public LeaderInfo( String leaderElectionGroupName) {
    _leaderElectionGroupName = leaderElectionGroupName;
    _record = new DataRecord(_leaderElectionGroupName);
  }

  public enum LeaderAttribute {
    LEADER_NAME,
    PARTICIPANTS
  }


}
