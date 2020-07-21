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
 * RoutingDataReaderType is an enum that designates the reader type and the class name that can be
 * used to create an instance of RoutingDataReader by reflection.
 */
public enum RoutingDataReaderType {
  HTTP("org.apache.helix.zookeeper.routing.HttpRoutingDataReader"),
  ZK("org.apache.helix.zookeeper.routing.ZkRoutingDataReader"),
  HTTP_ZK_FALLBACK("org.apache.helix.zookeeper.routing.HttpZkFallbackRoutingDataReader");

  private final String className;

  RoutingDataReaderType(String className) {
    this.className = className;
  }

  public String getClassName() {
    return this.className;
  }
}
