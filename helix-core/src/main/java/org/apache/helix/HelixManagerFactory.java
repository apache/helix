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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Obtain one of a set of Helix cluster managers, organized by the backing system.
 * factory that creates cluster managers.
 */
public final class HelixManagerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HelixManagerFactory.class);

  /**
   * Construct a zk-based cluster manager that enforces all types (PARTICIPANT, CONTROLLER, and
   * SPECTATOR) to have a name
   * @param clusterName
   * @param instanceName
   * @param type
   * @param zkAddr
   * @return a HelixManager backed by Zookeeper
   */
  public static HelixManager getZKHelixManager(String clusterName, String instanceName,
      InstanceType type, String zkAddr) {
    return new ZKHelixManager(clusterName, instanceName, type, zkAddr);
  }

  /**
   * Construct a zk-based cluster manager that enforces all types (PARTICIPANT, CONTROLLER, and
   * SPECTATOR) to have a name
   * @param clusterName
   * @param instanceName
   * @param type
   * @param zkAddr
   * @param stateListener
   * @return a HelixManager backed by Zookeeper
   */
  public static HelixManager getZKHelixManager(String clusterName, String instanceName,
      InstanceType type, String zkAddr, HelixManagerStateListener stateListener) {
    return new ZKHelixManager(clusterName, instanceName, type, zkAddr, stateListener);
  }

  /**
   * Construct a ZkHelixManager using the HelixManagerProperty instance given.
   * HelixManagerProperty given must contain a valid ZkConnectionConfig.
   * @param clusterName
   * @param instanceName
   * @param type
   * @param stateListener
   * @param helixManagerProperty must contain a valid ZkConnectionConfig
   * @return
   */
  public static HelixManager getZKHelixManager(String clusterName, String instanceName,
      InstanceType type, HelixManagerStateListener stateListener,
      HelixManagerProperty helixManagerProperty) {
    return new ZKHelixManager(clusterName, instanceName, type, null, stateListener,
        helixManagerProperty);
  }
}
