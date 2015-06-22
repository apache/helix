package org.apache.helix;

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

import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;

/**
 * helix-role i.e. participant, controller, auto-controller
 */
public interface HelixRole {
  /**
   * get the underlying connection
   * @return helix-connection
   */
  HelixConnection getConnection();

  /**
   * get cluster id to which this role belongs
   * @return cluster id
   */
  ClusterId getClusterId();

  /**
   * get id of this helix-role
   * @return id
   */
  Id getId();

  /**
   * helix-role type
   * @return
   */
  InstanceType getType();

  /**
   * get the messaging-service
   * @return messaging-service
   */
  ClusterMessagingService getMessagingService();

  /**
   * get data accessor
   * @return
   */
  HelixDataAccessor getAccessor();
}
