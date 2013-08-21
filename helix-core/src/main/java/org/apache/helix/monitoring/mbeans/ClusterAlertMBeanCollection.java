package org.apache.helix.monitoring.mbeans;

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

import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.alerts.AlertParser;
import org.apache.helix.alerts.AlertValueAndStatus;
import org.apache.helix.alerts.Tuple;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class ClusterAlertMBeanCollection {
  public static String DOMAIN_ALERT = "HelixAlerts";
  public static String ALERT_SUMMARY = "AlertSummary";

  private static final Logger _logger = Logger.getLogger(ClusterAlertMBeanCollection.class);
  ConcurrentHashMap<String, ClusterAlertItem> _alertBeans =
      new ConcurrentHashMap<String, ClusterAlertItem>();

  Map<String, String> _recentAlertDelta;
  ClusterAlertSummary _clusterAlertSummary;
  ZNRecord _alertHistory = new ZNRecord(PropertyType.ALERT_HISTORY.toString());
  Set<String> _previousFiredAlerts = new HashSet<String>();
  // 5 min for mbean freshness threshold
  public static final long ALERT_NOCHANGE_THRESHOLD = 5 * 60 * 1000;

  final MBeanServer _beanServer;

  public interface ClusterAlertSummaryMBean extends ClusterAlertItemMBean {
    public String getAlertFiredHistory();
  }

  class ClusterAlertSummary extends ClusterAlertItem implements ClusterAlertSummaryMBean {
    public ClusterAlertSummary(String name, AlertValueAndStatus valueAndStatus) {
      super(name, valueAndStatus);
    }

    /**
     * Returns the previous 100 alert mbean turn on / off history
     */
    @Override
    public String getAlertFiredHistory() {
      try {
        ObjectMapper mapper = new ObjectMapper();
        SerializationConfig serializationConfig = mapper.getSerializationConfig();
        serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
        StringWriter sw = new StringWriter();
        mapper.writeValue(sw, _alertHistory);
        return sw.toString();
      } catch (Exception e) {
        _logger.warn("", e);
        return "";
      }
    }
  }

  public ClusterAlertMBeanCollection() {
    _beanServer = ManagementFactory.getPlatformMBeanServer();
  }

  public Collection<ClusterAlertItemMBean> getCurrentAlertMBeans() {
    ArrayList<ClusterAlertItemMBean> beans = new ArrayList<ClusterAlertItemMBean>();
    for (ClusterAlertItem item : _alertBeans.values()) {
      beans.add(item);
    }
    return beans;
  }

  void onNewAlertMbeanAdded(ClusterAlertItemMBean bean) {
    try {
      _logger.info("alert bean " + bean.getSensorName() + " exposed to jmx");
      System.out.println("alert bean " + bean.getSensorName() + " exposed to jmx");
      ObjectName objectName = new ObjectName(DOMAIN_ALERT + ":alert=" + bean.getSensorName());
      register(bean, objectName);
    } catch (Exception e) {
      _logger.error("", e);
      e.printStackTrace();
    }
  }

  public void setAlerts(String originAlert, Map<String, AlertValueAndStatus> alertResultMap,
      String clusterName) {
    if (alertResultMap == null) {
      _logger.warn("null alertResultMap");
      return;
    }
    for (String alertName : alertResultMap.keySet()) {
      String beanName = "";
      if (alertName.length() > 1) {
        String comparator = AlertParser.getComponent(AlertParser.COMPARATOR_NAME, originAlert);
        String constant = AlertParser.getComponent(AlertParser.CONSTANT_NAME, originAlert);
        beanName = "(" + alertName + ")" + comparator + "(" + constant + ")";
      } else {
        beanName = originAlert + "--(" + alertName + ")";
      }
      // This is to make JMX happy; certain charaters cannot be in JMX bean name
      beanName = beanName.replace('*', '%').replace('=', '#').replace(',', ';');
      if (!_alertBeans.containsKey(beanName)) {
        ClusterAlertItem item = new ClusterAlertItem(beanName, alertResultMap.get(alertName));
        onNewAlertMbeanAdded(item);
        _alertBeans.put(beanName, item);
      } else {
        _alertBeans.get(beanName).setValueMap(alertResultMap.get(alertName));
      }
    }
    refreshSummayAlert(clusterName);
  }

  public void setAlertHistory(ZNRecord alertHistory) {
    _alertHistory = alertHistory;
  }

  /**
   * The summary alert is a combination of all alerts, if it is on, something is wrong on this
   * cluster. The additional info contains all alert mbean names that has been fired.
   */
  void refreshSummayAlert(String clusterName) {
    boolean fired = false;
    String alertsFired = "";
    String summaryKey = ALERT_SUMMARY + "_" + clusterName;
    for (String key : _alertBeans.keySet()) {
      if (!key.equals(summaryKey)) {
        ClusterAlertItem item = _alertBeans.get(key);
        fired = (item.getAlertFired() == 1) | fired;
        if (item.getAlertFired() == 1) {
          alertsFired += item._alertItemName;
          alertsFired += ";";
        }
      }
    }
    Tuple<String> t = new Tuple<String>();
    t.add("0");
    AlertValueAndStatus summaryStatus = new AlertValueAndStatus(t, fired);
    if (!_alertBeans.containsKey(summaryKey)) {
      ClusterAlertSummary item = new ClusterAlertSummary(summaryKey, summaryStatus);
      onNewAlertMbeanAdded(item);
      item.setAdditionalInfo(alertsFired);
      _alertBeans.put(summaryKey, item);
      _clusterAlertSummary = item;
    } else {
      _alertBeans.get(summaryKey).setValueMap(summaryStatus);
      _alertBeans.get(summaryKey).setAdditionalInfo(alertsFired);
    }
  }

  void register(Object bean, ObjectName name) {
    try {
      _beanServer.unregisterMBean(name);
    } catch (Exception e) {
    }
    try {
      _beanServer.registerMBean(bean, name);
    } catch (Exception e) {
      _logger.error("Could not register MBean", e);
    }
  }

  public void reset() {
    for (String beanName : _alertBeans.keySet()) {
      ClusterAlertItem item = _alertBeans.get(beanName);
      item.reset();
      try {
        ObjectName objectName = new ObjectName(DOMAIN_ALERT + ":alert=" + item.getSensorName());
        _beanServer.unregisterMBean(objectName);
      } catch (Exception e) {
        _logger.warn("", e);
      }
    }
    _alertBeans.clear();
  }

  public void refreshAlertDelta(String clusterName) {
    // Update the alert turn on/turn off history
    String summaryKey = ALERT_SUMMARY + "_" + clusterName;
    Set<String> currentFiredAlerts = new HashSet<String>();
    for (String key : _alertBeans.keySet()) {
      if (!key.equals(summaryKey)) {
        ClusterAlertItem item = _alertBeans.get(key);
        if (item.getAlertFired() == 1) {
          currentFiredAlerts.add(item._alertItemName);
        }
      }
    }

    Map<String, String> onOffAlertsMap = new HashMap<String, String>();
    for (String alertName : currentFiredAlerts) {
      if (!_previousFiredAlerts.contains(alertName)) {
        onOffAlertsMap.put(alertName, "ON");
        _logger.info(alertName + " ON");
        _previousFiredAlerts.add(alertName);
      }
    }
    for (String cachedAlert : _previousFiredAlerts) {
      if (!currentFiredAlerts.contains(cachedAlert)) {
        onOffAlertsMap.put(cachedAlert, "OFF");
        _logger.info(cachedAlert + " OFF");
      }
    }
    for (String key : onOffAlertsMap.keySet()) {
      if (onOffAlertsMap.get(key).equals("OFF")) {
        _previousFiredAlerts.remove(key);
      }
    }
    if (onOffAlertsMap.size() == 0) {
      _logger.info("No MBean change");
    }
    _recentAlertDelta = onOffAlertsMap;

    checkMBeanFreshness(ALERT_NOCHANGE_THRESHOLD);
  }

  public Map<String, String> getRecentAlertDelta() {
    return _recentAlertDelta;
  }

  /**
   * Remove mbeans that has not been changed for thresholdInMs MS
   */
  void checkMBeanFreshness(long thresholdInMs) {
    long now = new Date().getTime();
    Set<String> oldBeanNames = new HashSet<String>();
    // Get mbean items that has not been updated for thresholdInMs
    for (String beanName : _alertBeans.keySet()) {
      ClusterAlertItem item = _alertBeans.get(beanName);
      if (now - item.getLastUpdateTime() > thresholdInMs) {
        oldBeanNames.add(beanName);
        _logger.info("bean " + beanName + " has not been updated for " + thresholdInMs + " MS");
      }
    }
    for (String beanName : oldBeanNames) {
      ClusterAlertItem item = _alertBeans.get(beanName);
      _alertBeans.remove(beanName);
      try {
        item.reset();
        ObjectName objectName = new ObjectName(DOMAIN_ALERT + ":alert=" + item.getSensorName());
        _beanServer.unregisterMBean(objectName);
      } catch (Exception e) {
        _logger.warn("", e);
      }
    }
  }
}
