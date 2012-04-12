package com.linkedin.helix.controller.stages;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.alerts.AlertProcessor;
import com.linkedin.helix.alerts.AlertValueAndStatus;
import com.linkedin.helix.alerts.AlertsHolder;
import com.linkedin.helix.alerts.ExpressionParser;
import com.linkedin.helix.alerts.StatsHolder;
import com.linkedin.helix.alerts.Tuple;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageContext;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.healthcheck.StatHealthReportProvider;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.monitoring.mbeans.ClusterAlertMBeanCollection;

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

  StatsHolder _statsHolder;
  AlertsHolder _alertsHolder;
  Map<String, Map<String, AlertValueAndStatus>> _alertStatus;
  Map<String, Tuple<String>> _statStatus;
  ClusterAlertMBeanCollection _alertBeanCollection = new ClusterAlertMBeanCollection();

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

    DataAccessor accessor = manager.getDataAccessor();
    boolean retVal = accessor.setProperty(PropertyType.PERSISTENTSTATS, record);
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

    _statsHolder = new StatsHolder(manager, cache);
    _alertsHolder = new AlertsHolder(manager, cache, _statsHolder);

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
  
  void updateAlertHistory(HelixManager manager)
  {
 // Write alert fire history to zookeeper
    _alertBeanCollection.refreshAlertDelta(manager.getClusterName());
    Map<String, String> delta = _alertBeanCollection.getRecentAlertDelta();
    // Update history only when some beans has changed
    if(delta.size() > 0)
    {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-DD-hh:mm:ss");
      String date = dateFormat.format(new Date());
      
      ZNRecord alertFiredHistory = manager.getDataAccessor().getProperty(PropertyType.ALERT_HISTORY);
      if(alertFiredHistory == null)
      {
        alertFiredHistory = new ZNRecord(PropertyType.ALERT_HISTORY.toString());
      }
      while(alertFiredHistory.getMapFields().size() >= ALERT_HISTORY_SIZE)
      {
        // ZNRecord uses TreeMap which is sorted ascending internally
        String firstKey = (String)(alertFiredHistory.getMapFields().keySet().toArray()[0]);
        alertFiredHistory.getMapFields().remove(firstKey);
      }
      alertFiredHistory.setMapField(date, delta);
      manager.getDataAccessor().setProperty(PropertyType.ALERT_HISTORY, alertFiredHistory);
      _alertBeanCollection.setAlertHistory(alertFiredHistory);
    }
  }

  public ClusterAlertMBeanCollection getClusterAlertMBeanCollection()
  {
    return _alertBeanCollection;
  }
}
