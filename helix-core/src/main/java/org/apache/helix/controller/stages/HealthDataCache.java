/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.DataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.AlertStatus;
import org.apache.helix.model.Alerts;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.PersistentStats;


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
    _liveInstanceMap = accessor.getChildValuesMap(LiveInstance.class,
        PropertyType.LIVEINSTANCES);

    Map<String, Map<String, HealthStat>> hsMap = new HashMap<String, Map<String, HealthStat>>();

    for (String instanceName : _liveInstanceMap.keySet())
    {
      // xxx clearly getting znodes for the instance here...so get the
      // timestamp!

      hsMap.put(instanceName, accessor.getChildValuesMap(HealthStat.class,
          PropertyType.HEALTHREPORT, instanceName));
    }
    _healthStatMap = Collections.unmodifiableMap(hsMap);
    _persistentStats = accessor.getProperty(PersistentStats.class,
        PropertyType.PERSISTENTSTATS);
    _alerts = accessor.getProperty(Alerts.class, PropertyType.ALERTS);
    _alertStatus = accessor.getProperty(AlertStatus.class,
        PropertyType.ALERT_STATUS);

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

  public boolean refresh(HelixDataAccessor accessor)
  {
    Builder keyBuilder = accessor.keyBuilder();
    _liveInstanceMap = accessor.getChildValuesMap(keyBuilder.liveInstances());

    Map<String, Map<String, HealthStat>> hsMap = new HashMap<String, Map<String, HealthStat>>();

    for (String instanceName : _liveInstanceMap.keySet())
    {
      // xxx clearly getting znodes for the instance here...so get the
      // timestamp!

      Map<String, HealthStat> childValuesMap = accessor
          .getChildValuesMap(keyBuilder.healthReports(instanceName));
      hsMap.put(instanceName, childValuesMap);
    }
    _healthStatMap = Collections.unmodifiableMap(hsMap);
    _persistentStats = accessor.getProperty(keyBuilder.persistantStat());
    _alerts = accessor.getProperty(keyBuilder.alerts());
    _alertStatus = accessor.getProperty(keyBuilder.alertStatus());

    return true;

  }

}
