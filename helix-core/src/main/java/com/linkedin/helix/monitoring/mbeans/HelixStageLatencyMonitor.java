package com.linkedin.helix.monitoring.mbeans;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.helix.monitoring.StatCollector;

public class HelixStageLatencyMonitor implements HelixStageLatencyMonitorMBean
{
  private static final Logger LOG = Logger.getLogger(HelixStageLatencyMonitor.class);

  private final StatCollector _stgLatency;
  private final MBeanServer _beanServer;
  private final String _clusterName;
  private final String _stageName;
  private final ObjectName _objectName;

  public HelixStageLatencyMonitor(String clusterName, String stageName) throws Exception
  {
    _clusterName = clusterName;
    _stageName = stageName;
    _stgLatency = new StatCollector();
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    _objectName = new ObjectName("StageLatencyMonitor: " + "cluster=" + _clusterName + ",stage=" + _stageName);
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

  public void addStgLatency(long time)
  {
    _stgLatency.addData(time);
  }

  public void reset()
  {
    _stgLatency.reset();
    unregister(_objectName);
  }

  @Override
  public long getMaxStgLatency()
  {
    return (long) _stgLatency.getMax();
  }

  @Override
  public long getMeanStgLatency()
  {
    return (long) _stgLatency.getMean();
  }

  @Override
  public long get95StgLatency()
  {
    return (long) _stgLatency.getPercentile(95);
  }

}
