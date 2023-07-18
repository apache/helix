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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.helix.metaclient.datamodel.DataRecord;


/**
 * This is the data represent leader election info of a leader election path.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class LeaderInfo extends DataRecord {

  public LeaderInfo(DataRecord dataRecord) {
    super(dataRecord);
  }

  @JsonCreator
  public LeaderInfo(@JsonProperty("id") String id) {
    super(id);
  }

  public LeaderInfo(LeaderInfo info, String id) {
    super(info, id);
  }


  public enum LeaderAttribute {
    LEADER_NAME,
    PARTICIPANTS
  }

@JsonIgnore(true)
public String getLeaderName() {
    return getSimpleField("LEADER_NAME");
  }

  @JsonIgnore(true)
  public void setLeaderName(String id) {
     setSimpleField("LEADER_NAME", id);
  }


}
