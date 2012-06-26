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
package com.linkedin.helix;

import static com.linkedin.helix.PropertyType.ALERTS;
import static com.linkedin.helix.PropertyType.ALERT_STATUS;
import static com.linkedin.helix.PropertyType.CONFIGS;
import static com.linkedin.helix.PropertyType.CURRENTSTATES;
import static com.linkedin.helix.PropertyType.EXTERNALVIEW;
import static com.linkedin.helix.PropertyType.HEALTHREPORT;
import static com.linkedin.helix.PropertyType.HISTORY;
import static com.linkedin.helix.PropertyType.IDEALSTATES;
import static com.linkedin.helix.PropertyType.LIVEINSTANCES;
import static com.linkedin.helix.PropertyType.MESSAGES;
import static com.linkedin.helix.PropertyType.PAUSE;
import static com.linkedin.helix.PropertyType.STATEMODELDEFS;
import static com.linkedin.helix.PropertyType.STATUSUPDATES;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.linkedin.helix.model.AlertStatus;
import com.linkedin.helix.model.Alerts;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LeaderHistory;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.PauseSignal;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.model.StatusUpdate;

public class PropertyPathConfig
{
  private static Logger logger = Logger.getLogger(PropertyPathConfig.class);

  static Map<PropertyType, Map<Integer, String>> templateMap = new HashMap<PropertyType, Map<Integer, String>>();
  static Map<PropertyType, Class<? extends HelixProperty>> typeToClassMapping= new HashMap<PropertyType, Class<? extends HelixProperty>>();
  static{
    typeToClassMapping.put(LIVEINSTANCES, LiveInstance.class);
    typeToClassMapping.put(IDEALSTATES, IdealState.class);
    typeToClassMapping.put(CONFIGS, InstanceConfig.class);
    typeToClassMapping.put(EXTERNALVIEW, ExternalView.class);
    typeToClassMapping.put(STATEMODELDEFS, StateModelDefinition.class);
    typeToClassMapping.put(MESSAGES, Message.class);
    typeToClassMapping.put(CURRENTSTATES, CurrentState.class);
    typeToClassMapping.put(STATUSUPDATES, StatusUpdate.class);
    typeToClassMapping.put(HISTORY, LeaderHistory.class);
    typeToClassMapping.put(HEALTHREPORT, HealthStat.class);
    typeToClassMapping.put(ALERTS, Alerts.class);
    typeToClassMapping.put(ALERT_STATUS, AlertStatus.class);
    typeToClassMapping.put(PAUSE, PauseSignal.class);
  }
  static
  {
    // @formatter:off
    addEntry(PropertyType.CONFIGS, 1, "/{clusterName}/CONFIGS");
    addEntry(PropertyType.CONFIGS, 2, "/{clusterName}/CONFIGS/{scope}");
    addEntry(PropertyType.CONFIGS, 3, "/{clusterName}/CONFIGS/{scope}/{scopeKey}");
    // addEntry(PropertyType.CONFIGS,2,"/{clusterName}/CONFIGS/{instanceName}");
    addEntry(PropertyType.LIVEINSTANCES, 1, "/{clusterName}/LIVEINSTANCES");
    addEntry(PropertyType.LIVEINSTANCES, 2, "/{clusterName}/LIVEINSTANCES/{instanceName}");
    addEntry(PropertyType.INSTANCES, 1, "/{clusterName}/INSTANCES");
    addEntry(PropertyType.INSTANCES, 2, "/{clusterName}/INSTANCES/{instanceName}");
    addEntry(PropertyType.IDEALSTATES, 1, "/{clusterName}/IDEALSTATES");
    addEntry(PropertyType.IDEALSTATES, 2, "/{clusterName}/IDEALSTATES/{resourceName}");
    addEntry(PropertyType.EXTERNALVIEW, 1, "/{clusterName}/EXTERNALVIEW");
    addEntry(PropertyType.EXTERNALVIEW, 2, "/{clusterName}/EXTERNALVIEW/{resourceName}");
    addEntry(PropertyType.STATEMODELDEFS, 1, "/{clusterName}/STATEMODELDEFS");
    addEntry(PropertyType.STATEMODELDEFS, 2, "/{clusterName}/STATEMODELDEFS/{stateModelName}");
    addEntry(PropertyType.CONTROLLER, 1, "/{clusterName}/CONTROLLER");
    addEntry(PropertyType.PROPERTYSTORE, 1, "/{clusterName}/PROPERTYSTORE");
    // INSTANCE
    addEntry(PropertyType.MESSAGES, 2, "/{clusterName}/INSTANCES/{instanceName}/MESSAGES");
    addEntry(PropertyType.MESSAGES, 3, "/{clusterName}/INSTANCES/{instanceName}/MESSAGES/{msgId}");
    addEntry(PropertyType.CURRENTSTATES, 2, "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES");
    addEntry(PropertyType.CURRENTSTATES, 3,
        "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}");
    addEntry(PropertyType.CURRENTSTATES, 4,
        "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}/{resourceName}");
    addEntry(PropertyType.STATUSUPDATES, 2, "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES");
    addEntry(PropertyType.STATUSUPDATES, 3,
        "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}");
    addEntry(PropertyType.STATUSUPDATES, 4,
        "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}");
    addEntry(PropertyType.STATUSUPDATES, 5,
        "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}/{recordName}");
    addEntry(PropertyType.ERRORS, 2, "/{clusterName}/INSTANCES/{instanceName}/ERRORS");
    addEntry(PropertyType.ERRORS, 3, "/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}");
    addEntry(PropertyType.ERRORS, 4,
        "/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}/{subPath}");
    addEntry(PropertyType.ERRORS, 5,
        "/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}/{subPath}/{recordName}");
    addEntry(PropertyType.HEALTHREPORT, 2, "/{clusterName}/INSTANCES/{instanceName}/HEALTHREPORT");
    addEntry(PropertyType.HEALTHREPORT, 3,
        "/{clusterName}/INSTANCES/{instanceName}/HEALTHREPORT/{reportName}");
    // CONTROLLER
    addEntry(PropertyType.MESSAGES_CONTROLLER, 1, "/{clusterName}/CONTROLLER/MESSAGES");
    addEntry(PropertyType.MESSAGES_CONTROLLER, 2, "/{clusterName}/CONTROLLER/MESSAGES/{msgId}");
    addEntry(PropertyType.ERRORS_CONTROLLER, 1, "/{clusterName}/CONTROLLER/ERRORS");
    addEntry(PropertyType.ERRORS_CONTROLLER, 2, "/{clusterName}/CONTROLLER/ERRORS/{errorId}");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 1, "/{clusterName}/CONTROLLER/STATUSUPDATES");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 2,
        "/{clusterName}/CONTROLLER/STATUSUPDATES/{sessionId}");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 3,
        "/{clusterName}/CONTROLLER/STATUSUPDATES/{sessionId}/{subPath}");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 4,
        "/{clusterName}/CONTROLLER/STATUSUPDATES/{sessionId}/{subPath}/{recordName}");
    addEntry(PropertyType.LEADER, 1, "/{clusterName}/CONTROLLER/LEADER");
    addEntry(PropertyType.HISTORY, 1, "/{clusterName}/CONTROLLER/HISTORY");
    addEntry(PropertyType.PAUSE, 1, "/{clusterName}/CONTROLLER/PAUSE");
    addEntry(PropertyType.PERSISTENTSTATS, 1, "/{clusterName}/CONTROLLER/PERSISTENTSTATS");
    addEntry(PropertyType.ALERTS, 1, "/{clusterName}/CONTROLLER/ALERTS");
    addEntry(PropertyType.ALERT_STATUS, 1, "/{clusterName}/CONTROLLER/ALERT_STATUS");
    addEntry(PropertyType.ALERT_HISTORY, 1, "/{clusterName}/CONTROLLER/ALERT_HISTORY");
    // @formatter:on

  }
  static Pattern pattern = Pattern.compile("(\\{.+?\\})");

  private static void addEntry(PropertyType type, int numKeys, String template)
  {
    if (!templateMap.containsKey(type))
    {
      templateMap.put(type, new HashMap<Integer, String>());
    }
    logger.trace("Adding template for type:" + type.getType() + " arguments:" + numKeys
        + " template:" + template);
    templateMap.get(type).put(numKeys, template);
  }

//  public static String getPath(PropertyType type, String... keys)
//  {
//    String clusterName = keys[0];
//    String[] subKeys = Arrays.copyOfRange(keys, 1, keys.length);
//    return getPathInternal(type, clusterName, subKeys);
//  }
  
  public static String getPath(PropertyType type, String clusterName, String... keys)
  {
    if (clusterName == null)
    {
      logger.warn("ClusterName can't be null for type:" + type);
      return null;
    }
    if (keys == null)
    {
      keys = new String[] {};
    }
    String template = null;
    if (templateMap.containsKey(type))
    {
      // keys.length+1 since we add clusterName
      template = templateMap.get(type).get(keys.length + 1);
    }

    String result = null;

    if (template != null)
    {
      result = template;
      Matcher matcher = pattern.matcher(template);
      int count = 0;
      while (matcher.find())
      {
        count = count + 1;
        String var = matcher.group();
        if (count == 1)
        {
          result = result.replace(var, clusterName);
        } else
        {
          result = result.replace(var, keys[count - 2]);
        }
      }
    }
    if (result == null || result.indexOf('{') > -1 || result.indexOf('}') > -1)
    {
      logger.warn("Unable to instantiate template:" + template + " using clusterName:"
          + clusterName + " and keys:" + Arrays.toString(keys));
    }
    return result;
  }
}