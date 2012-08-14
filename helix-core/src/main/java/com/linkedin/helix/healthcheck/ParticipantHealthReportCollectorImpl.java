/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.healthcheck;

import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.alerts.StatsHolder;
import com.linkedin.helix.model.HealthStat;

public class ParticipantHealthReportCollectorImpl implements
    ParticipantHealthReportCollector
{
  private final LinkedList<HealthReportProvider> _healthReportProviderList = new LinkedList<HealthReportProvider>();
  private Timer _timer;
  private static final Logger _logger = Logger
      .getLogger(ParticipantHealthReportCollectorImpl.class);
  private final HelixManager _helixManager;
  String _instanceName;
  public final static int DEFAULT_REPORT_LATENCY = 60 * 1000;

  public ParticipantHealthReportCollectorImpl(HelixManager helixManager,
      String instanceName)
  {
    _helixManager = helixManager;
    _instanceName = instanceName;
    addDefaultHealthCheckInfoProvider();
  }

  private void addDefaultHealthCheckInfoProvider()
  {
    addHealthReportProvider(new DefaultHealthReportProvider());
  }

  public void start()
  {
    if (_timer == null)
    {
      _timer = new Timer(true);
      _timer.scheduleAtFixedRate(new HealthCheckInfoReportingTask(),
          new Random().nextInt(DEFAULT_REPORT_LATENCY), DEFAULT_REPORT_LATENCY);
    }
    else
    {
      _logger.warn("timer already started");
    }
  }

  @Override
  public void addHealthReportProvider(HealthReportProvider provider)
  {
    try
    {
      synchronized (_healthReportProviderList)
      {
        if (!_healthReportProviderList.contains(provider))
        {
          _healthReportProviderList.add(provider);
        }
        else
        {
          _logger.warn("Skipping a duplicated HealthCheckInfoProvider");
        }
      }
    }
    catch (Exception e)
    {
      _logger.error(e);
    }
  }

  @Override
  public void removeHealthReportProvider(HealthReportProvider provider)
  {
    synchronized (_healthReportProviderList)
    {
      if (_healthReportProviderList.contains(provider))
      {
        _healthReportProviderList.remove(provider);
      }
      else
      {
        _logger.warn("Skip removing a non-exist HealthCheckInfoProvider");
      }
    }
  }

  @Override
  public void reportHealthReportMessage(ZNRecord healthCheckInfoUpdate)
  {
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
//    accessor.setProperty(
//        PropertyType.HEALTHREPORT, healthCheckInfoUpdate, _instanceName,
//        healthCheckInfoUpdate.getId());
    accessor.setProperty(keyBuilder.healthReport(_instanceName, healthCheckInfoUpdate.getId()), 
                         new HealthStat(healthCheckInfoUpdate));

  }

  public void stop()
  {
    _logger.info("Stop HealthCheckInfoReportingTask");
    if (_timer != null)
    {
      _timer.cancel();
      _timer = null;
    }
    else
    {
      _logger.warn("timer already stopped");
    }
  }

  @Override
  public synchronized void transmitHealthReports()
  {
    synchronized (_healthReportProviderList)
    {
      for (HealthReportProvider provider : _healthReportProviderList)
      {
        try
        {
          Map<String, String> report = provider.getRecentHealthReport();
          Map<String, Map<String, String>> partitionReport = provider
              .getRecentPartitionHealthReport();
          ZNRecord record = new ZNRecord(provider.getReportName());
          if (report != null)
          {
            record.setSimpleFields(report);
          }
          if (partitionReport != null)
          {
            record.setMapFields(partitionReport);
          }
          record.setSimpleField(StatsHolder.TIMESTAMP_NAME, "" + System.currentTimeMillis());
          
          HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
          Builder keyBuilder = accessor.keyBuilder();
          accessor.setProperty(keyBuilder.healthReport(_instanceName, record.getId()), 
                               new HealthStat(record));

//          _helixManager.getDataAccessor().setProperty(
//              PropertyType.HEALTHREPORT, record, _instanceName, record.getId());
          // reset stats (for now just the partition stats)
          provider.resetStats();
        }
        catch (Exception e)
        {
          _logger.error("", e);
        }
      }
    }
  }

  class HealthCheckInfoReportingTask extends TimerTask
  {
    @Override
    public void run()
    {
      transmitHealthReports();
    }
  }
}
