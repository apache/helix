package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.alerts.AlertParser;
import org.apache.helix.alerts.AlertProcessor;
import org.apache.helix.alerts.AlertValueAndStatus;
import org.apache.helix.alerts.AlertsHolder;
import org.apache.helix.alerts.ExpressionParser;
import org.apache.helix.alerts.StatsHolder;
import org.apache.helix.alerts.Tuple;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.healthcheck.StatHealthReportProvider;
import org.apache.helix.manager.zk.DefaultParticipantErrorMessageHandlerFactory.ActionOnError;
import org.apache.helix.model.AlertHistory;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.PersistentStats;
import org.apache.helix.monitoring.mbeans.ClusterAlertMBeanCollection;
import org.apache.log4j.Logger;


/**
 * For each LiveInstances select currentState and message whose sessionId matches
 * sessionId from LiveInstance Get Partition,State for all the resources computed in
 * previous State [ResourceComputationStage]
 *
 * @author asilbers
 *
 */
public class StatsAggregationStage extends AbstractBaseStage
{

  public static final int ALERT_HISTORY_SIZE = 30;

  private static final Logger logger =
      Logger.getLogger(StatsAggregationStage.class.getName());

  StatsHolder _statsHolder = null;
  AlertsHolder _alertsHolder = null;
  Map<String, Map<String, AlertValueAndStatus>> _alertStatus;
  Map<String, Tuple<String>> _statStatus;
  ClusterAlertMBeanCollection _alertBeanCollection = new ClusterAlertMBeanCollection();
  Map<String, String> _alertActionTaken = new HashMap<String, String>();

  public final String PARTICIPANT_STAT_REPORT_NAME = StatHealthReportProvider.REPORT_NAME;
  public final String ESPRESSO_STAT_REPORT_NAME = "RestQueryStats";
  public final String REPORT_NAME = "AggStats";
  // public final String DEFAULT_AGG_TYPE = "decay";
  // public final String DEFAULT_DECAY_PARAM = "0.1";
  // public final String DEFAULT_AGG_TYPE = "window";
  // public final String DEFAULT_DECAY_PARAM = "5";

  public StatHealthReportProvider _aggStatsProvider;

  // public AggregationType _defaultAggType;

  public Map<String, Map<String, AlertValueAndStatus>> getAlertStatus()
  {
    return _alertStatus;
  }

  public Map<String, Tuple<String>> getStatStatus()
  {
    return _statStatus;
  }

  public void persistAggStats(HelixManager manager)
  {
    Map<String, String> report = _aggStatsProvider.getRecentHealthReport();
    Map<String, Map<String, String>> partitionReport =
        _aggStatsProvider.getRecentPartitionHealthReport();
    ZNRecord record = new ZNRecord(_aggStatsProvider.getReportName());
    if (report != null)
    {
      record.setSimpleFields(report);
    }
    if (partitionReport != null)
    {
      record.setMapFields(partitionReport);
    }

//    DataAccessor accessor = manager.getDataAccessor();
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
//    boolean retVal = accessor.setProperty(PropertyType.PERSISTENTSTATS, record);
    Builder keyBuilder = accessor.keyBuilder();
    boolean retVal = accessor.setProperty(keyBuilder.persistantStat(), new PersistentStats(record));
    if (retVal == false)
    {
      logger.error("attempt to persist derived stats failed");
    }
  }

  @Override
  public void init(StageContext context)
  {
  }

  public String getAgeStatName(String instance)
  {
    return instance + ExpressionParser.statFieldDelim + "reportingage";
  }

  // currTime in seconds
  public void reportAgeStat(LiveInstance instance, long modifiedTime, long currTime)
  {
    String statName = getAgeStatName(instance.getInstanceName());
    long age = (currTime - modifiedTime) / 1000; // XXX: ensure this is in
                                                 // seconds
    Map<String, String> ageStatMap = new HashMap<String, String>();
    ageStatMap.put(StatsHolder.TIMESTAMP_NAME, String.valueOf(currTime));
    ageStatMap.put(StatsHolder.VALUE_NAME, String.valueOf(age));
    // note that applyStat will only work if alert already added
    _statsHolder.applyStat(statName, ageStatMap);
  }

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    long startTime = System.currentTimeMillis();
    // String aggTypeName =
    // DEFAULT_AGG_TYPE+AggregationType.DELIM+DEFAULT_DECAY_PARAM;
    // _defaultAggType = AggregationTypeFactory.getAggregationType(aggTypeName);

    HelixManager manager = event.getAttribute("helixmanager");
    HealthDataCache cache = event.getAttribute("HealthDataCache");

    if (manager == null || cache == null)
    {
      throw new StageException("helixmanager|HealthDataCache attribute value is null");
    }
    if(_alertsHolder == null)
    {
      _statsHolder = new StatsHolder(manager, cache);
      _alertsHolder = new AlertsHolder(manager, cache, _statsHolder);
    }
    else
    {
      _statsHolder.updateCache(cache);
      _alertsHolder.updateCache(cache);
    }
    if (_statsHolder.getStatsList().size() == 0)
    {
      logger.info("stat holder is empty");
      return;
    }

    // init agg stats from cache
    // initAggStats(cache);

    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();

    long currTime = System.currentTimeMillis();
    // for each live node, read node's stats
    long readInstancesStart = System.currentTimeMillis();
    for (LiveInstance instance : liveInstances.values())
    {
      String instanceName = instance.getInstanceName();
      logger.debug("instanceName: " + instanceName);
      // XXX: now have map of HealthStats, so no need to traverse them...verify
      // correctness
      Map<String, HealthStat> stats;
      stats = cache.getHealthStats(instanceName);
      // find participants stats
      long modTime = -1;
      // TODO: get healthreport child node modified time and reportAgeStat based on that
      boolean reportedAge = false;
      for (HealthStat participantStat : stats.values())
      {
        if (participantStat != null && !reportedAge)
        {
          // generate and report stats for how old this node's report is
          modTime = participantStat.getLastModifiedTimeStamp();
          reportAgeStat(instance, modTime, currTime);
          reportedAge = true;
        }
        // System.out.println(modTime);
        // XXX: need to convert participantStat to a better format
        // need to get instanceName in here

        if (participantStat != null)
        {
          // String timestamp = String.valueOf(instance.getModifiedTime()); WANT
          // REPORT LEVEL TS
          Map<String, Map<String, String>> statMap =
              participantStat.getHealthFields(instanceName);
          for (String key : statMap.keySet())
          {
            _statsHolder.applyStat(key, statMap.get(key));
          }
        }
      }
    }
    // Call _statsHolder.persistStats() once per pipeline. This will
    // write the updated persisted stats into zookeeper
    _statsHolder.persistStats();
    logger.info("Done processing stats: "
        + (System.currentTimeMillis() - readInstancesStart));
    // populate _statStatus
    _statStatus = _statsHolder.getStatsMap();

    for (String statKey : _statStatus.keySet())
    {
      logger.debug("Stat key, value: " + statKey + ": " + _statStatus.get(statKey));
    }

    long alertExecuteStartTime = System.currentTimeMillis();
    // execute alerts, populate _alertStatus
    _alertStatus =
        AlertProcessor.executeAllAlerts(_alertsHolder.getAlertList(),
                                        _statsHolder.getStatsList());
    logger.info("done executing alerts: "
        + (System.currentTimeMillis() - alertExecuteStartTime));
    for (String originAlertName : _alertStatus.keySet())
    {
      _alertBeanCollection.setAlerts(originAlertName,
                                     _alertStatus.get(originAlertName),
                                     manager.getClusterName());
    }

    executeAlertActions(manager);
    // Write alert fire history to zookeeper
    updateAlertHistory(manager);
    long writeAlertStartTime = System.currentTimeMillis();
    // write out alert status (to zk)
    _alertsHolder.addAlertStatusSet(_alertStatus);
    logger.info("done writing alerts: "
        + (System.currentTimeMillis() - writeAlertStartTime));

    // TODO: access the 2 status variables from somewhere to populate graphs

    long logAlertStartTime = System.currentTimeMillis();
    // logging alert status
    for (String alertOuterKey : _alertStatus.keySet())
    {
      logger.debug("Alert Outer Key: " + alertOuterKey);
      Map<String, AlertValueAndStatus> alertInnerMap = _alertStatus.get(alertOuterKey);
      if (alertInnerMap == null)
      {
        logger.debug(alertOuterKey + " has no alerts to report.");
        continue;
      }
      for (String alertInnerKey : alertInnerMap.keySet())
      {
        logger.debug("  " + alertInnerKey + " value: "
            + alertInnerMap.get(alertInnerKey).getValue() + ", status: "
            + alertInnerMap.get(alertInnerKey).isFired());
      }
    }

    logger.info("done logging alerts: "
        + (System.currentTimeMillis() - logAlertStartTime));

    long processLatency = System.currentTimeMillis() - startTime;
    addLatencyToMonitor(event, processLatency);
    logger.info("process end: " + processLatency);
  }

  /**
   * Go through the _alertStatus, and call executeAlertAction for those actual alerts that
   * has been fired
   */

  void executeAlertActions( HelixManager manager)
  {
    _alertActionTaken.clear();
    // Go through the original alert strings
    for(String originAlertName : _alertStatus.keySet())
    {
      Map<String, String> alertFields = _alertsHolder.getAlertsMap().get(originAlertName);
      if(alertFields != null && alertFields.containsKey(AlertParser.ACTION_NAME))
      {
        String actionValue = alertFields.get(AlertParser.ACTION_NAME);
        Map<String, AlertValueAndStatus> alertResultMap = _alertStatus.get(originAlertName);
        if(alertResultMap == null)
        {
          logger.info("Alert "+ originAlertName + " does not have alert status map");
          continue;
        }
        // For each original alert, iterate all actual alerts that it expands into
        for(String actualStatName : alertResultMap.keySet())
        {
          // if the actual alert is fired, execute the action
          if(alertResultMap.get(actualStatName).isFired())
          {
            logger.warn("Alert " + originAlertName + " action " + actionValue + " is triggered by " + actualStatName);
            _alertActionTaken.put(actualStatName, actionValue);
            // move functionalities into a seperate class
            executeAlertAction(actualStatName, actionValue, manager);
          }
        }
      }
    }
  }
  /**
   * Execute the action if an alert is fired, and the alert has an action associated with it.
   * NOTE: consider unify this with DefaultParticipantErrorMessageHandler.handleMessage()
   */
  void executeAlertAction(String actualStatName, String actionValue, HelixManager manager)
  {
    if(actionValue.equals(ActionOnError.DISABLE_INSTANCE.toString()))
    {
      String instanceName = parseInstanceName(actualStatName, manager);
      if(instanceName != null)
      {
        logger.info("Disabling instance " + instanceName);
        manager.getClusterManagmentTool().enableInstance(manager.getClusterName(), instanceName, false);
      }
    }
    else if(actionValue.equals(ActionOnError.DISABLE_PARTITION.toString()))
    {
      String instanceName = parseInstanceName(actualStatName, manager);
      String resourceName = parseResourceName(actualStatName, manager);
      String partitionName = parsePartitionName(actualStatName, manager);
      if(instanceName != null && resourceName != null && partitionName != null)
      {
        logger.info("Disabling partition " + partitionName + " instanceName " +  instanceName);
        manager.getClusterManagmentTool().enablePartition(false, manager.getClusterName(), instanceName,
            resourceName, Arrays.asList(partitionName));
      }
    }
    else if(actionValue.equals(ActionOnError.DISABLE_RESOURCE.toString()))
    {
      String instanceName = parseInstanceName(actualStatName, manager);
      String resourceName = parseResourceName(actualStatName, manager);
      logger.info("Disabling resource " + resourceName + " instanceName " +  instanceName + " not implemented");

    }
  }

  public static String parseResourceName(String actualStatName, HelixManager manager)
  {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder kb = accessor.keyBuilder();
    List<IdealState> idealStates = accessor.getChildValues(kb.idealStates());
    for (IdealState idealState : idealStates)
    {
      String resourceName = idealState.getResourceName();
      if(actualStatName.contains("=" + resourceName + ".") || actualStatName.contains("=" + resourceName + ";"))
      {
        return resourceName;
      }
    }
    return null;
  }

  public static String parsePartitionName(String actualStatName, HelixManager manager)
  {
    String resourceName = parseResourceName(actualStatName, manager);
    if(resourceName != null)
    {
      String partitionKey = "=" + resourceName + "_";
      if(actualStatName.contains(partitionKey))
      {
        int pos = actualStatName.indexOf(partitionKey);
        int nextDotPos = actualStatName.indexOf('.', pos + partitionKey.length());
        int nextCommaPos = actualStatName.indexOf(';', pos + partitionKey.length());
        if(nextCommaPos > 0 && nextCommaPos < nextDotPos)
        {
          nextDotPos = nextCommaPos;
        }

        String partitionName = actualStatName.substring(pos + 1, nextDotPos);
        return partitionName;
      }
    }
    return null;
  }

  public static String parseInstanceName(String actualStatName, HelixManager manager)
  {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    Builder kb = accessor.keyBuilder();
    List<LiveInstance> liveInstances = accessor.getChildValues(kb.liveInstances());
    for (LiveInstance instance : liveInstances)
    {
      String instanceName = instance.getInstanceName();
      if(actualStatName.startsWith(instanceName))
      {
        return instanceName;
      }
    }
    return null;
  }

  void updateAlertHistory(HelixManager manager)
  {
   // Write alert fire history to zookeeper
    _alertBeanCollection.refreshAlertDelta(manager.getClusterName());
    Map<String, String> delta = _alertBeanCollection.getRecentAlertDelta();
    // Update history only when some beans has changed
    if(delta.size() > 0)
    {
      delta.putAll(_alertActionTaken);
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh:mm:ss:SSS");
      String date = dateFormat.format(new Date());

      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();

      HelixProperty property = accessor.getProperty(keyBuilder.alertHistory());
      ZNRecord alertFiredHistory;
      if(property == null)
      {
        alertFiredHistory = new ZNRecord(PropertyType.ALERT_HISTORY.toString());
      }
      else
      {
        alertFiredHistory = property.getRecord();
      }
      while(alertFiredHistory.getMapFields().size() >= ALERT_HISTORY_SIZE)
      {
        // ZNRecord uses TreeMap which is sorted ascending internally
        String firstKey = (String)(alertFiredHistory.getMapFields().keySet().toArray()[0]);
        alertFiredHistory.getMapFields().remove(firstKey);
      }
      alertFiredHistory.setMapField(date, delta);
//      manager.getDataAccessor().setProperty(PropertyType.ALERT_HISTORY, alertFiredHistory);
      accessor.setProperty(keyBuilder.alertHistory(), new AlertHistory(alertFiredHistory));
      _alertBeanCollection.setAlertHistory(alertFiredHistory);
    }
  }

  public ClusterAlertMBeanCollection getClusterAlertMBeanCollection()
  {
    return _alertBeanCollection;
  }
}
