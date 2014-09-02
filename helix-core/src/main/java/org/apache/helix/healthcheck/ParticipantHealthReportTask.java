package org.apache.helix.healthcheck;

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

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.helix.HelixTimerTask;
import org.apache.log4j.Logger;

public class ParticipantHealthReportTask extends HelixTimerTask {
  private static final Logger LOG = Logger.getLogger(ParticipantHealthReportTask.class);
  public final static int DEFAULT_REPORT_LATENCY = 60 * 1000;

  Timer _timer;
  final ParticipantHealthReportCollectorImpl _healthReportCollector;

  class ParticipantHealthReportTimerTask extends TimerTask {

    @Override
    public void run() {
      _healthReportCollector.transmitHealthReports();
    }
  }

  public ParticipantHealthReportTask(ParticipantHealthReportCollectorImpl healthReportCollector) {
    _healthReportCollector = healthReportCollector;
  }

  @Override
  public void start() {
    if (_timer == null) {
      LOG.info("Start HealthCheckInfoReportingTask");
      _timer = new Timer("ParticipantHealthReportTimerTask", true);
      _timer.scheduleAtFixedRate(new ParticipantHealthReportTimerTask(),
          new Random().nextInt(DEFAULT_REPORT_LATENCY), DEFAULT_REPORT_LATENCY);
    } else {
      LOG.warn("ParticipantHealthReportTimerTask already started");
    }
  }

  @Override
  public void stop() {
    if (_timer != null) {
      LOG.info("Stop ParticipantHealthReportTimerTask");
      _timer.cancel();
      _timer = null;
    } else {
      LOG.warn("ParticipantHealthReportTimerTask already stopped");
    }
  }

}