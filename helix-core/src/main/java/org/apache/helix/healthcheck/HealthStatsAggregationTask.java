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


public class HealthStatsAggregationTask extends HelixTimerTask
{  
  private static final Logger LOG = Logger.getLogger(HealthStatsAggregationTask.class);
  public final static int DEFAULT_HEALTH_CHECK_LATENCY = 30 * 1000;
  
  final HealthStatsAggregator _healthStatsAggregator;
  
  class HealthStatsAggregationTaskTimer extends TimerTask {

    @Override
    public void run() {
      _healthStatsAggregator.aggregate();
    }
    
  }
  
  private Timer _timer;
  private final int _delay;
  private final int _period;
  
  public HealthStatsAggregationTask(HealthStatsAggregator healthStatsAggregator, int delay, int period)
  {
    _healthStatsAggregator = healthStatsAggregator;
    
    _delay = delay;
    _period = period;
  }

  public HealthStatsAggregationTask(HealthStatsAggregator healthStatsAggregator)
  {
    this(healthStatsAggregator, 
        DEFAULT_HEALTH_CHECK_LATENCY, DEFAULT_HEALTH_CHECK_LATENCY);
  }

  @Override
  public void start()
  {

    if (_timer == null)
    {
      LOG.info("START HealthStatsAggregationTimerTask");

      // Remove all the previous health check values, if any
      _healthStatsAggregator.init();
      
      _timer = new Timer("HealthStatsAggregationTimerTask", true);
      _timer.scheduleAtFixedRate(new HealthStatsAggregationTaskTimer(), 
          new Random().nextInt(_delay), _period);
    }
    else
    {
      LOG.warn("HealthStatsAggregationTimerTask already started");
    }
  }

  @Override
  public synchronized void stop()
  {
    if (_timer != null)
    {
      LOG.info("Stop HealthStatsAggregationTimerTask");
      _timer.cancel();
      _healthStatsAggregator.reset();
      _timer = null;
    }
    else
    {
      LOG.warn("HealthStatsAggregationTimerTask already stopped");
    }
  }

}
