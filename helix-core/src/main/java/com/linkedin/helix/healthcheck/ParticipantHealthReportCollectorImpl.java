package com.linkedin.helix.healthcheck;

import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;

public class ParticipantHealthReportCollectorImpl implements
    ParticipantHealthReportCollector
{
  private final LinkedList<HealthReportProvider> _healthReportProviderList = new LinkedList<HealthReportProvider>();
  private Timer _timer;
  private static final Logger _logger = Logger
      .getLogger(ParticipantHealthReportCollectorImpl.class);
  private final ClusterManager _clusterManager;
  String _instanceName;
  public final static int DEFAULT_REPORT_LATENCY = 60 * 1000;

  public ParticipantHealthReportCollectorImpl(ClusterManager clusterManager,
      String instanceName)
  {
    _clusterManager = clusterManager;
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
      _timer = new Timer();
      _timer.scheduleAtFixedRate(new HealthCheckInfoReportingTask(),
          new Random().nextInt(DEFAULT_REPORT_LATENCY), DEFAULT_REPORT_LATENCY);
    } else
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
        } else
        {
          _logger.warn("Skipping a duplicated HealthCheckInfoProvider");
        }
      }
    } catch (Exception e)
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
      } else
      {
        _logger.warn("Skip removing a non-exist HealthCheckInfoProvider");
      }
    }
  }

  //XXX: think about getting rid of method argument
  @Override
  public void reportHealthReportMessage(ZNRecord healthCheckInfoUpdate)
  {
    transmitHealthReports();
  }

  public void stop()
  {
    _logger.info("Stop HealthCheckInfoReportingTask");
    if (_timer != null)
    {
      _timer.cancel();
      _timer = null;
    } else
    {
      _logger.warn("timer already stopped");
    }
  }

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
            if (report != null) {
            	record.setSimpleFields(report);
            }
            if (partitionReport != null) {
            	record.setMapFields(partitionReport);
            }
            
            _clusterManager.getDataAccessor().setProperty(
                PropertyType.HEALTHREPORT, record, _instanceName,
                record.getId());
            //reset stats (for now just the partition stats)
            provider.resetStats();
          } catch (Exception e)
          {
            _logger.error(e);
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
    	/*
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
            if (report != null) {
            	record.setSimpleFields(report);
            }
            if (partitionReport != null) {
            	record.setMapFields(partitionReport);
            }
            _clusterManager.getDataAccessor().setProperty(PropertyType.HEALTHREPORT,
                                                          record,
                                                          _instanceName,
                                                          record.getId());
            //reset stats (for now just the partition stats)
            provider.resetStats();
          } catch (Exception e)
          {
            _logger.error(e);
          }
        }
      }
      */
    }
  }
}
