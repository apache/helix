package org.apache.helix.agent;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.helix.agent.SystemUtil.ProcessStateCode;
import org.apache.log4j.Logger;

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
 * thread for monitoring a pid
 */
public class ProcessMonitorThread extends Thread {
  private static final Logger LOG = Logger.getLogger(ProcessMonitorThread.class);
  private static final int MONITOR_PERIOD_BASE = 1000; // 1 second

  private final String _pid;

  public ProcessMonitorThread(String pid) {
    _pid = pid;
  }

  @Override
  public void run() {

    // monitor pid
    try {
      ProcessStateCode processState = SystemUtil.getProcessState(_pid);
      while (processState != null) {
        if (processState == ProcessStateCode.Z) {
          LOG.error("process: " + _pid + " is in zombie state");
          break;
        }
        TimeUnit.MILLISECONDS
            .sleep(new Random().nextInt(MONITOR_PERIOD_BASE) + MONITOR_PERIOD_BASE);
        processState = SystemUtil.getProcessState(_pid);
      }
    } catch (Exception e) {
      LOG.error("fail to monitor process: " + _pid, e);
    }

    // TODO need to find the exit value of pid and kill the pid on timeout
  }

}
