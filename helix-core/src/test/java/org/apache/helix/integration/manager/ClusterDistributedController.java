package org.apache.helix.integration.manager;

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

import org.apache.helix.InstanceType;
import org.apache.helix.participant.DistClusterControllerStateModelFactory;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterDistributedController extends ClusterManager {
  private static Logger LOG = LoggerFactory.getLogger(ClusterDistributedController.class);

  public ClusterDistributedController(String zkAddr, String clusterName, String controllerName) {
    super(zkAddr, clusterName, controllerName, InstanceType.CONTROLLER_PARTICIPANT);
  }

  @Override
  public void run() {
    try {
      StateMachineEngine stateMach = getStateMachineEngine();
      DistClusterControllerStateModelFactory lsModelFactory =
          new DistClusterControllerStateModelFactory(_zkAddress);
      stateMach.registerStateModelFactory("LeaderStandby", lsModelFactory);

      connect();
      _startCountDown.countDown();
      _stopCountDown.await();
    } catch (Exception e) {
      LOG.error("exception running controller-manager", e);
    } finally {
      _startCountDown.countDown();
      disconnect();
      _waitStopFinishCountDown.countDown();
    }
  }

  @Override
  public void finalize() {
    super.finalize();
  }
}
