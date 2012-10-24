package org.apache.helix.monitoring.mbeans;

import org.apache.helix.monitoring.StatCollector;
import org.apache.log4j.Logger;


public class MessageQueueMonitor implements MessageQueueMonitorMBean
{
  private static final Logger LOG = Logger.getLogger(MessageQueueMonitor.class);

  private final StatCollector _messageQueueSizeStat;
  private final String        _clusterName;
  private final String        _instanceName;

  public MessageQueueMonitor(String clusterName, String instanceName)
  {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _messageQueueSizeStat = new StatCollector();
  }


  public void addMessageQueueSize(long size)
  {
    _messageQueueSizeStat.addData(size);
  }

  public void reset()
  {
    _messageQueueSizeStat.reset();
  }

  @Override
  public double getMaxMessageQueueSize()
  {
    return _messageQueueSizeStat.getMax();
  }

  @Override
  public double getMeanMessageQueueSize()
  {
    return _messageQueueSizeStat.getMean();
  }

  @Override
  public String getSensorName()
  {
    return ClusterStatusMonitor.MESSAGE_QUEUE_STATUS_KEY + "_" + _clusterName + "_"
        + _instanceName;
  }
}
