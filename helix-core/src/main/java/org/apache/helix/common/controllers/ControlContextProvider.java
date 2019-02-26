package org.apache.helix.common.controllers;

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
 * An interface that provide controller contexts
 */
public interface ControlContextProvider {

  /**
   * Get the name of the cluster this control pipeline is responsible for
   * @return clusterName
   */
  String getClusterName();

  /**
   * Get the id of the cluster event that is currently being processed
   * @return event id
   */
  String getClusterEventId();

  /**
   * Set the id of the cluster event that is currently being processed
   * @param eventId event id
   */
  void setClusterEventId(String eventId);

  /**
   * Get control pipeline (controller) name
   * @return
   */
  String getPipelineName();
}
