package org.apache.helix.monitoring.mbeans;

import javax.management.JMException;
import javax.management.ObjectName;

public class ControllerStageMonitor implements ControllerStageMonitorMBean {
  public static final String DOMAIN = "CLusterStatus";
  public static final String CONTROLLER_STAGE = "ControllerStage";

  private ObjectName _objectName;
  private long _processedCounter;
  private long _processedLatencyCounter;

  public ControllerStageMonitor(String stageName) throws JMException {
    register(stageName);
  }

  public void increaseProcessingCounter(long time) {
    _processedCounter++;
    _processedLatencyCounter += time;
  }

  private void register(String stage) throws JMException {
    _objectName = MBeanRegistrar.register(this, DOMAIN, CONTROLLER_STAGE, stage);
  }

  public void unregister() {
    MBeanRegistrar.unregister(_objectName);
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s", DOMAIN, _objectName.getKeyProperty(CONTROLLER_STAGE));
  }

  @Override
  public long getStageProcessedCounter() {
    return _processedCounter;
  }

  @Override
  public long getStageProcessedLatencyCounter() {
    return _processedLatencyCounter;
  }
}
