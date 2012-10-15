package com.linkedin.helix.monitoring.mbeans;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.helix.monitoring.StatCollector;

public class MessageQueueMonitor implements MessageQueueMonitorMBean
{
  private static final Logger LOG = Logger.getLogger(MessageQueueMonitor.class);

  private final StatCollector _messageQueueSize;
  private final MBeanServer   _beanServer;
  private final String        _clusterName;
  private final String        _instanceName;
  private ObjectName          _objectName;

  public MessageQueueMonitor(String clusterName, String instanceName)
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _messageQueueSize = new StatCollector();
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    try
    {
      _objectName =
          new ObjectName(ClusterStatusMonitor.CLUSTER_STATUS_KEY + ": "
              + ClusterStatusMonitor.CLUSTER_DN_KEY + "=" + _clusterName + ","
              + ClusterStatusMonitor.INSTANCE_DN_KEY + "=" + _instanceName);
      register(this, _objectName);
    }
    catch (Exception e)
    {
      LOG.error("fail to register mbean: " + _objectName, e);
    }
  }

  private void register(Object bean, ObjectName name) throws Exception
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
      // OK
    }

    try
    {
      _beanServer.registerMBean(bean, name);
    }
    catch (Exception e)
    {
      LOG.warn("fail toregister mbean: " + name, e);
    }

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
      LOG.error("fail to unregister mbean: " + _objectName, e);
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

  @Override
  public String getSensorName()
  {
    return ClusterStatusMonitor.MESSAGE_QUEUE_STATUS_KEY + "_" + _clusterName + "_"
        + _instanceName;
  }
}
