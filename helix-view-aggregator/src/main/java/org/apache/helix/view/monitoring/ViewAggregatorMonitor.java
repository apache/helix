package org.apache.helix.view.monitoring;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

public class ViewAggregatorMonitor extends DynamicMBeanProvider {
  /* package */ static final String MBEAN_DOMAIN = MonitorDomainNames.ClusterStatus.name();
  /* package */ static final String MONITOR_KEY = "ViewClusterName";
  private static final String MBEAN_DESCRIPTION = "Monitor helix view aggregator activity";
  private final String _clusterName;
  private final String _sensorName;

  // Counters
  private final SimpleDynamicMetric<Long> _refreshViewFailureCounter;
  private final SimpleDynamicMetric<Long> _sourceReadFailureCounter;
  private final SimpleDynamicMetric<Long> _processViewConfigFailureCounter;
  private final SimpleDynamicMetric<Long> _processedSourceClusterEventCounter;

  // Gauges
  private final HistogramDynamicMetric _viewRefreshLatencyGauge;

  public ViewAggregatorMonitor(String clusterName) {
    _clusterName = clusterName;
    _sensorName = String.format("%s.%s.%s", MBEAN_DOMAIN, MONITOR_KEY, clusterName);

    // Initialize metrics
    _refreshViewFailureCounter =
        new SimpleDynamicMetric<>("ViewClusterRefreshFailureCounter", 0L);
    _sourceReadFailureCounter =
        new SimpleDynamicMetric<>("SourceClusterRefreshFailureCounter", 0L);
    _processedSourceClusterEventCounter =
        new SimpleDynamicMetric<>("ProcessedSourceClusterEventCounter", 0L);
    _processViewConfigFailureCounter =
        new SimpleDynamicMetric<>("ProcessViewConfigFailureCounter", 0L);
    _viewRefreshLatencyGauge = new HistogramDynamicMetric("ViewClusterRefreshDurationGauge",
        new Histogram(
            new SlidingTimeWindowArrayReservoir(DEFAULT_RESET_INTERVAL_MS, TimeUnit.MILLISECONDS)));
  }

  public void recordViewRefreshFailure() {
    _refreshViewFailureCounter.updateValue(_refreshViewFailureCounter.getValue() + 1);
  }

  public void recordViewConfigProcessFailure() {
    _processViewConfigFailureCounter.updateValue(_processViewConfigFailureCounter.getValue() + 1);
  }

  public void recordReadSourceFailure() {
    _sourceReadFailureCounter.updateValue(_sourceReadFailureCounter.getValue() + 1);
  }

  public void recordProcessedSourceEvent() {
    _processedSourceClusterEventCounter
        .updateValue(_processedSourceClusterEventCounter.getValue() + 1);
  }

  public void recordRefreshViewLatency(long latency) {
    _viewRefreshLatencyGauge.updateValue(latency);
  }

  @Override
  public String getSensorName() {
    return _sensorName;
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_sourceReadFailureCounter);
    attributeList.add(_refreshViewFailureCounter);
    attributeList.add(_processViewConfigFailureCounter);
    attributeList.add(_processedSourceClusterEventCounter);
    attributeList.add(_viewRefreshLatencyGauge);

    doRegister(attributeList, MBEAN_DESCRIPTION, MBeanRegistrar
        .buildObjectName(MBEAN_DOMAIN, MONITOR_KEY, _clusterName));
    return this;
  }
}
