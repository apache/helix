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

public class RoutingDataConstants {

  /**
   * Default interval that defines how frequently RoutingDataManager's routing data should be
   * updated from the routing data source. This exists to apply throttling to the rate at which
   * the ZkClient pulls routing data from the routing data source to avoid overloading the routing
   * data source.
   */
  public static final long DEFAULT_ROUTING_DATA_UPDATE_INTERVAL_MS = 5 * 1000L; // 5 seconds
}
