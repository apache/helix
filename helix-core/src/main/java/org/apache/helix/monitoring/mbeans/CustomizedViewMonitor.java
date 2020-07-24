package org.apache.helix.monitoring.mbeans;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.Partition;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.collections.Lists;


public class CustomizedViewMonitor extends DynamicMBeanProvider {
  private static Logger LOG = LoggerFactory.getLogger(CustomizedViewMonitor.class);

  private static final String MBEAN_DESCRIPTION = "Helix Customized View Aggregation Monitor";
  private final String _clusterName;
  private final String _sensorName;
  private HistogramDynamicMetric _updateToAggregationLatencyGauge;

  public CustomizedViewMonitor(String clusterName) {
    _clusterName = clusterName;
    _sensorName =
        String.format("%s.%s", MonitorDomainNames.RoutingTableProvider.name(), _clusterName);
    _updateToAggregationLatencyGauge = new HistogramDynamicMetric("UpdateToAggregationLatencyGauge",
        new Histogram(
            new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_updateToAggregationLatencyGauge);
    doRegister(attributeList, MBEAN_DESCRIPTION, getMBeanName());
    return this;
  }

  private ObjectName getMBeanName() throws MalformedObjectNameException {
    return new ObjectName(String
        .format("%s:%s=%s", MonitorDomainNames.CustomizedView.name(), "Cluster", _clusterName));
  }

  @Override
  public String getSensorName() {
    return _sensorName;
  }

  private void recordUpdateToAggregationLatency(long latency) {
    if (_updateToAggregationLatencyGauge != null) {
      _updateToAggregationLatencyGauge.updateValue(latency);
    }
  }

  /**
   * Find updated customized states and report the aggregation latency of each customized state
   * @param updatedCustomizedViews Customized views that have been updated, obtained from CustomizedStateOutput
   * @param curCustomizedViews Current customized view values from the CustomizedViewCache
   * @param updatedStartTimestamps All customized state START_TIME property values from CustomizedStateOutput
   * @param updateSuccess If the customized view update to ZK is successful or not
   * @param endTime The timestamp when the new customized view is updated to ZK
   */
  public void reportLatency(List<CustomizedView> updatedCustomizedViews,
      Map<String, CustomizedView> curCustomizedViews,
      Map<String, Map<Partition, Map<String, String>>> updatedStartTimestamps,
      boolean[] updateSuccess, long endTime) {
    for (int i = 0; i < updatedCustomizedViews.size(); i++) {
      if (!updateSuccess[i]) {
        continue;
      }
      CustomizedView updatedCustomizedView = updatedCustomizedViews.get(i);
      String resourceName = updatedCustomizedView.getResourceName();
      CustomizedView curCustomizedView =
          curCustomizedViews.getOrDefault(resourceName, new CustomizedView(resourceName));
      List<Long> startTimestamps = getStartTimestamps(updatedCustomizedView, curCustomizedView,
          updatedStartTimestamps.get(resourceName));
      startTimestamps.forEach(startTime -> recordUpdateToAggregationLatency(endTime - startTime));
    }
  }

  private List<Long> getStartTimestamps(CustomizedView newCV, CustomizedView oldCV,
      Map<Partition, Map<String, String>> partitionStartTimestamps) {
    List<Long> collectedTimestamps = Lists.newArrayList();

    if (newCV == null || oldCV == null || partitionStartTimestamps == null) {
      LOG.warn(
          "Cannot find updated time stamps for customized state at resource level, input parameter is null.");
      return collectedTimestamps;
    }
    MapDifference<String, Map<String, String>> diff =
        Maps.difference(newCV.getRecord().getMapFields(), oldCV.getRecord().getMapFields());

    // Resource level value diff
    Map<String, MapDifference.ValueDifference<Map<String, String>>> resourceDiffs =
        diff.entriesDiffering();
    if (resourceDiffs != null) {
      resourceDiffs.forEach((key, value) -> {
        compareAtPartitionLevel(key, value.leftValue(), value.rightValue(),
            partitionStartTimestamps, collectedTimestamps);
      });
    }

    // Resource level left only - newly added resource
    Map<String, Map<String, String>> leftOnlyResourceStateMap = diff.entriesOnlyOnLeft();
    if (leftOnlyResourceStateMap != null) {
      leftOnlyResourceStateMap.forEach((key, value) -> {
        compareAtPartitionLevel(key, value, Maps.newHashMap(), partitionStartTimestamps,
            collectedTimestamps);
      });
    }

    // Resource level right only - newly deleted resource
    Map<String, Map<String, String>> rightOnlyResourceStateMap = diff.entriesOnlyOnRight();
    if (rightOnlyResourceStateMap != null) {
      rightOnlyResourceStateMap.forEach((key, value) -> {
        compareAtPartitionLevel(key, Maps.newHashMap(), value, partitionStartTimestamps,
            collectedTimestamps);
      });
    }

    return collectedTimestamps;
  }

  private void compareAtPartitionLevel(String resourceName, Map<String, String> newStateMap,
      Map<String, String> oldStateMap, Map<Partition, Map<String, String>> partitionStartTimeMaps,
      List<Long> collectedTimestamps) {
    if (newStateMap == null || oldStateMap == null || partitionStartTimeMaps == null
        || collectedTimestamps == null) {
      LOG.warn(
          "Cannot find updated time stamps for customized state at partition level, input parameter is null.");
      return;
    }

    Map<String, String> partitionStartTimeMap =
        partitionStartTimeMaps.getOrDefault(new Partition(resourceName), Collections.emptyMap());
    // Partition level comparison
    MapDifference<String, String> stateMapDiff = Maps.difference(newStateMap, oldStateMap);

    // Partition level value diff
    Map<String, MapDifference.ValueDifference<String>> stateMapValueDiff =
        stateMapDiff.entriesDiffering();
    if (stateMapValueDiff != null) {
      stateMapValueDiff
          .forEach((key, value) -> parseTimestamp(partitionStartTimeMap, key, collectedTimestamps));
    }

    // Partition level left only -> newly added state
    Map<String, String> leftOnlyStateMapDiff = stateMapDiff.entriesOnlyOnLeft();
    if (leftOnlyStateMapDiff != null) {
      leftOnlyStateMapDiff
          .forEach((key, value) -> parseTimestamp(partitionStartTimeMap, key, collectedTimestamps));
    }

    // Partition level right only -> newly deleted state
    Map<String, String> rightOnlyStateMapDiff = stateMapDiff.entriesOnlyOnRight();
    if (rightOnlyStateMapDiff != null) {
      rightOnlyStateMapDiff
          .forEach((key, value) -> parseTimestamp(partitionStartTimeMap, key, collectedTimestamps));
    }
  }

  private void parseTimestamp(Map<String, String> source, String instanceName,
      List<Long> collectedTimestamps) {
    if (source.get(instanceName) != null) {
      try {
        long timestamp = Long.parseLong(source.get(instanceName));
        if (timestamp > 0) {
          collectedTimestamps.add(timestamp);
          return;
        }
      } catch (NumberFormatException e) {
        LOG.warn("Error occurs while parsing customized state update time stamp");
      }
      LOG.warn(
          "Failed to report latency, customized state is updated but no update time stamp found.");
    }
  }
}
