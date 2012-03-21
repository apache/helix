package com.linkedin.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.model.AlertStatus;
import com.linkedin.helix.model.Alerts;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.PersistentStats;

public class HealthDataCache
{
  Map<String, LiveInstance> _liveInstanceMap;

  Map<String, Map<String, HealthStat>> _healthStatMap;
  HealthStat _globalStats; // DON'T THINK I WILL USE THIS ANYMORE
  PersistentStats _persistentStats;
  Alerts _alerts;
  AlertStatus _alertStatus;

  public boolean refresh(DataAccessor accessor)
  {
    _liveInstanceMap =
        accessor.getChildValuesMap(LiveInstance.class, PropertyType.LIVEINSTANCES);

    Map<String, Map<String, HealthStat>> hsMap =
        new HashMap<String, Map<String, HealthStat>>();
    for (String instanceName : _liveInstanceMap.keySet())
    {
      // xxx clearly getting znodes for the instance here...so get the
      // timestamp!
      hsMap.put(instanceName, accessor.getChildValuesMap(HealthStat.class,
                                                         PropertyType.HEALTHREPORT,
                                                         instanceName));
    }
    _healthStatMap = Collections.unmodifiableMap(hsMap);
    _persistentStats =
        accessor.getProperty(PersistentStats.class, PropertyType.PERSISTENTSTATS);
    _alerts = accessor.getProperty(Alerts.class, PropertyType.ALERTS);
    _alertStatus = accessor.getProperty(AlertStatus.class, PropertyType.ALERT_STATUS);

    return true;
  }

  public HealthStat getGlobalStats()
  {
    return _globalStats;
  }

  public PersistentStats getPersistentStats()
  {
    return _persistentStats;
  }

  public Alerts getAlerts()
  {
    return _alerts;
  }

  public AlertStatus getAlertStatus()
  {
    return _alertStatus;
  }

  public Map<String, HealthStat> getHealthStats(String instanceName)
  {
    Map<String, HealthStat> map = _healthStatMap.get(instanceName);
    if (map != null)
    {
      return map;
    } else
    {
      return Collections.emptyMap();
    }
  }

  public Map<String, LiveInstance> getLiveInstances()
  {
    return _liveInstanceMap;
  }

}
