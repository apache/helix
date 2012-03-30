package com.linkedin.helix.monitoring.mbeans;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.helix.monitoring.StatCollector;

public class HelixStageLatencyMonitor implements HelixStageLatencyMonitorMBean
{
  private static final Logger LOG = Logger.getLogger(HelixStageLatencyMonitor.class);

  private final StatCollector _healthStatAggStgTime;
  private final MBeanServer _beanServer;
  private final String _clusterName;
  private final ObjectName _objectName;

  public HelixStageLatencyMonitor(String clusterName) throws Exception
  {
    _clusterName = clusterName;
    _healthStatAggStgTime = new StatCollector();
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    _objectName = new ObjectName("StageTimeMonitor: " + "cluster=" + _clusterName);
    try
    {
      register(this, _objectName);
    }
    catch (Exception e)
    {
      LOG.error("Couldn't register " + _objectName + " mbean", e);
      throw e;
    }
  }

  private void register(Object bean, ObjectName name) throws Exception
  {
    try
    {
      _beanServer.unregisterMBean(name);
    }
    catch (Exception e)
    {
      // OK
    }

    _beanServer.registerMBean(bean, name);
  }

  private void unregister(ObjectName name)
  {
    try
    {
      _beanServer.unregisterMBean(name);
    }
    catch (Exception e)
    {
      LOG.error("Couldn't unregister " + _objectName + " mbean", e);
    }
  }

  public void addHeathStatAggStgTime(long time)
  {
    _healthStatAggStgTime.addData(time);
  }

  @Override
  public long getMaxHealthStatsAggStgLatency()
  {
    return (long) _healthStatAggStgTime.getMax();
  }

  public void reset()
  {
    _healthStatAggStgTime.reset();
    unregister(_objectName);
  }

  @Override
  public long getMeanHealthStatsAggStgLatency()
  {
    return (long) _healthStatAggStgTime.getMean();
  }

  @Override
  public long get95HealthStatsAggStgLatency()
  {
    return (long) _healthStatAggStgTime.getPercentile(95);
  }

}
