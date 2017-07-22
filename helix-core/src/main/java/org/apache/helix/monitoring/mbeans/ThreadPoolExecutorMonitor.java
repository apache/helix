package org.apache.helix.monitoring.mbeans;

import java.util.concurrent.ThreadPoolExecutor;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.helix.InstanceType;

public class ThreadPoolExecutorMonitor implements ThreadPoolExecutorMonitorMBean {
  public static final String TYPE = "Type";

  private ObjectName _objectName;
  private ThreadPoolExecutor _executor;

  public ThreadPoolExecutorMonitor(String type, ThreadPoolExecutor executor)
      throws JMException {
    _objectName = MBeanRegistrar
        .register(this, MonitorDomainNames.HelixThreadPoolExecutor.name(), TYPE, type);
    _executor = executor;
  }

  public void unregister() {
    MBeanRegistrar.unregister(_objectName);
  }

  @Override
  public String getSensorName() {
    if (_objectName.getKeyProperty(MBeanRegistrar.DUPLICATE) == null) {
      return String
          .format("%s.%s", _objectName.getDomain(), _objectName.getKeyProperty(TYPE));
    }
    return String.format("%s.%s.%s", _objectName.getDomain(),
        _objectName.getKeyProperty(TYPE), _objectName.getKeyProperty(MBeanRegistrar.DUPLICATE));
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
