package org.apache.helix.zookeeper.constant;

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
 * This class contains various routing-related system property keys for multi-zk clients.
 */
public class RoutingSystemPropertyKeys {

  /**
   * If enabled, FederatedZkClient (multiZkClient) will invalidate the cached routing data and
   * re-read the routing data from the routing data source upon ZK path sharding key cache miss.
   */
  public static final String UPDATE_ROUTING_DATA_ON_CACHE_MISS =
      "routing.data.update.on.cache.miss.enabled";

  /**
   * The interval to use between routing data updates from the routing data source.
   */
  public static final String ROUTING_DATA_UPDATE_INTERVAL_MS = "routing.data.update.interval.ms";
}
