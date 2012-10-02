package com.linkedin.helix.monitoring.mbeans;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.helix.monitoring.StatCollector;

public class HelixMessageQueueMonitor implements HelixMessageQueueMonitorMBean
{
  private static final Logger LOG = Logger.getLogger(HelixMessageQueueMonitor.class);

  private final StatCollector _messageQueueSize;
  private final MBeanServer _beanServer;
  private final String _clusterName;
  private ObjectName _objectName;

  public HelixMessageQueueMonitor(String clusterName)
  {
    _clusterName = clusterName;
    _messageQueueSize = new StatCollector();
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    try
    {
      _objectName = new ObjectName("HelixMessageQueueMonitor: cluster=" + _clusterName);
      register(this, _objectName);
    }
    catch (Exception e)
    {
      LOG.error("Couldn't register " + _objectName + " mbean", e);
      // throw e;
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
      if (_beanServer.isRegistered(name))
      {
        _beanServer.unregisterMBean(name);
      }
    }
    catch (Exception e)
    {
      LOG.error("Couldn't unregister " + _objectName + " mbean", e);
    }
  }

  public void addMessageQueueSize(long size)
  {
    _messageQueueSize.addData(size);
  }

  public void reset()
  {
    _messageQueueSize.reset();
    unregister(_objectName);
  }

  @Override
  public double getMaxMessageQueueSize()
  {
    return _messageQueueSize.getMax();
  }

  @Override
  public double getMeanMessageQueueSize()
  {
    return _messageQueueSize.getMean();
  }

}
