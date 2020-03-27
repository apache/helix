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

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;


public class SystemPropertyKeys {
  // Task Driver
  public static final String TASK_CONFIG_LIMITATION = "helixTask.configsLimitation";

  // ZKHelixManager
  public static final String CLUSTER_MANAGER_VERSION = "cluster-manager-version.properties";

  // soft constraints weight definitions
  public static final String SOFT_CONSTRAINT_WEIGHTS = "soft-constraint-weight.properties";

  public static final String FLAPPING_TIME_WINDOW = "helixmanager.flappingTimeWindow";

  // max disconnect count during the flapping time window to trigger HelixManager flapping handling
  public static final String MAX_DISCONNECT_THRESHOLD = "helixmanager.maxDisconnectThreshold";

  public static final String ZK_SESSION_TIMEOUT = "zk.session.timeout";

  public static final String ZK_CONNECTION_TIMEOUT = "zk.connection.timeout";

  @Deprecated
  public static final String ZK_REESTABLISHMENT_CONNECTION_TIMEOUT =
      "zk.connectionReEstablishment.timeout";

  public static final String ZK_WAIT_CONNECTED_TIMEOUT = "helixmanager.waitForConnectedTimeout";

  public static final String PARTICIPANT_HEALTH_REPORT_LATENCY =
      "helixmanager.participantHealthReport.reportLatency";

  // Indicate monitoring level of the HelixManager metrics
  public static final String MONITOR_LEVEL = "helixmanager.monitorLevel";

  // CallbackHandler
  public static final String ASYNC_BATCH_MODE_ENABLED = "helix.callbackhandler.isAsyncBatchModeEnabled";

  public static final String LEGACY_ASYNC_BATCH_MODE_ENABLED = "isAsyncBatchModeEnabled";

  // Controller
  public static final String CONTROLLER_MESSAGE_PURGE_DELAY = "helix.controller.stages.MessageGenerationPhase.messagePurgeDelay";

  // MBean monitor for helix.
  public static final String HELIX_MONITOR_TIME_WINDOW_LENGTH_MS = "helix.monitor.slidingTimeWindow.ms";

  // Multi-ZK mode enable/disable flag
  public static final String MULTI_ZK_ENABLED = "helix.multiZkEnabled";

  // System Property Metadata Store Directory Server endpoint key
  public static final String MSDS_SERVER_ENDPOINT_KEY =
      MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY;
}
