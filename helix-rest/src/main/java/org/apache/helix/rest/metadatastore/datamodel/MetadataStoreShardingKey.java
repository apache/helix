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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * A POJO class that represents a sharding key can be easily converted to JSON
 * in REST API response. The JSON object for a sharding key looks like:
 * {
 * 	"shardingKey": "/sharding/key/10/abc",
 * 	"realm": "realm.github.com"
 * }
 */
@JsonAutoDetect
public class MetadataStoreShardingKey {
  private String shardingKey;
  private String realm;

  @JsonCreator
  public MetadataStoreShardingKey(@JsonProperty String shardingKey, @JsonProperty String realm) {
    this.shardingKey = shardingKey;
    this.realm = realm;
  }

  @JsonProperty
  public String getShardingKey() {
    return shardingKey;
  }

  @JsonProperty
  public String getRealm() {
    return realm;
  }

  @Override
  public String toString() {
    return "MetadataStoreShardingKey{" + "shardingKey='" + shardingKey + '\'' + ", realm='" + realm
        + '\'' + '}';
  }
}
