package org.apache.helix.rest.metadatastore;

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

import java.util.Map;

public interface RoutingData {
  /**
   * Given a zkPath, return all the "sharding key-zkRealmAddress" pairs where the sharding keys
   * contain the given zkPath. For example, given "/ESPRESSO_MT_LD-1/schemata", return
   * {"/ESPRESSO_MT_LD-1/schemata/ESPRESSO_MT_LD-1": "zk-ltx1-0-espresso.prod.linkedin.com:2181",
   * "/ESPRESSO_MT_LD-1/schemata/ESPRESSO_MT_LD-2": "zk-ltx1-1-espresso.prod.linkedin.com:2181"}.
   * If the zkPath is invalid, returns an empty mapping.
   * @param zkPath - the zkPath where the search is conducted
   * @return all "sharding key-zkRealmAddress" pairs where the sharding keys contain the given
   *         zkPath if the zkPath is valid; empty mapping otherwise
   */
  Map<String, String> getAllMappingUnderPath(String zkPath);

  /**
   * Given a zkPath, return the zkRealmAddress corresponding to the sharding key contained in the
   * zkPath. If the zkPath doesn't contain a sharding key, throw IllegalArgumentException.
   * @param zkPath - the zkPath where the search is conducted
   * @return the zkRealmAddress corresponding to the sharding key contained in the zkPath
   * @throws IllegalArgumentException - when the zkPath doesn't contain a sharding key
   */
  String getZkRealm(String zkPath) throws IllegalArgumentException;
}
