package org.apache.helix.monitoring.mbeans;

import java.util.concurrent.ThreadPoolExecutor;
import javax.management.JMException;
import javax.management.ObjectName;

public class ThreadPoolExecutorMonitor implements ThreadPoolExecutorMonitorMBean {
  public static final String TYPE = "Type";

  private ObjectName _objectName;
  private ThreadPoolExecutor _executor;
  private String _type;

  public ThreadPoolExecutorMonitor(String type, ThreadPoolExecutor executor)
      throws JMException {
    _type = type;
    _executor = executor;
    _objectName = MBeanRegistrar
        .register(this, MonitorDomainNames.HelixThreadPoolExecutor.name(), TYPE, type);
  }

  public void unregister() {
    MBeanRegistrar.unregister(_objectName);
  }

  @Override
  public String getSensorName() {
    return String
        .format("%s.%s", MonitorDomainNames.HelixThreadPoolExecutor.name(), _type);
  }

  @Override
  public int getThreadPoolCoreSizeGauge() {
    return _executor.getCorePoolSize();
  }

  @Override
  public int getThreadPoolMaxSizeGauge() {
    return _executor.getMaximumPoolSize();
  }

  @Override
  public int getQueueSizeGauge() {
    return _executor.getQueue().size();
  }

  @Override
  public int getNumOfActiveThreadsGauge() {
    return _executor.getActiveCount();
  }
}
