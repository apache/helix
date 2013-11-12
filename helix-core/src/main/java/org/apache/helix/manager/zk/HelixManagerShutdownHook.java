package org.apache.helix.manager.zk;

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

import org.apache.helix.HelixManager;
import org.apache.log4j.Logger;

/**
 * Shutdown hook for helix manager
 * Working for kill -2/-15
 * NOT working for kill -9
 */
public class HelixManagerShutdownHook extends Thread {
  private static Logger LOG = Logger.getLogger(HelixManagerShutdownHook.class);

  final HelixManager _manager;

  public HelixManagerShutdownHook(HelixManager manager) {
    _manager = manager;
  }

  @Override
  public void run() {
    LOG.info("HelixControllerMainShutdownHook invoked on manager: " + _manager.getClusterName()
        + ", " + _manager.getInstanceName());
    _manager.disconnect();
  }
}
