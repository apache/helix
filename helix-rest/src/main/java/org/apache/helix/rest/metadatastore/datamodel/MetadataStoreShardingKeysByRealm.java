package org.apache.helix.rest.metadatastore.datamodel;

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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


@JsonPropertyOrder({"realm", "shardingKeys"})
public class MetadataStoreShardingKeysByRealm {
  private String realm;
  private List<String> shardingKeys;

  @JsonCreator
  public MetadataStoreShardingKeysByRealm(@JsonProperty String realm,
      @JsonProperty List<String> shardingKeys) {
    this.realm = realm;
    this.shardingKeys = shardingKeys;
  }

  @JsonProperty
  public String getRealm() {
    return realm;
  }

  @JsonProperty
  public List<String> getShardingKeys() {
    return shardingKeys;
  }

  @Override
  public String toString() {
    return "MetadataStoreShardingKeysByRealm{" + "realm='" + realm + '\'' + ", shardingKeys="
        + shardingKeys + '}';
  }
}
