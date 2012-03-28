package com.linkedin.helix.monitoring.mbeans;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.helix.alerts.AlertParser;
import com.linkedin.helix.alerts.AlertValueAndStatus;
import com.linkedin.helix.alerts.Tuple;

public class ClusterAlertMBeanCollection
{
  public static String DOMAIN_ALERT = "HelixAlerts";
  public static String ALERT_SUMMARY = "AlertSummary";
  
  private static final Logger _logger = Logger.getLogger(ClusterAlertMBeanCollection.class);
  ConcurrentHashMap<String, ClusterAlertItem> _alertBeans 
    = new ConcurrentHashMap<String, ClusterAlertItem>();
  
  final MBeanServer _beanServer;
  
  public ClusterAlertMBeanCollection()
  {
    _beanServer = ManagementFactory.getPlatformMBeanServer();
  }
  
  public Collection<ClusterAlertItemMBean> getCurrentAlertMBeans()
  {
    ArrayList<ClusterAlertItemMBean> beans = new ArrayList<ClusterAlertItemMBean>();
    for(ClusterAlertItem item : _alertBeans.values())
    {
      beans.add(item);
    }
    return beans;
  }

  void onNewAlertMbeanAdded(ClusterAlertItemMBean bean)
  {
    try
    {
      _logger.info("alert bean " + bean.getSensorName()+" exposed to jmx");
      System.out.println("alert bean " + bean.getSensorName()+" exposed to jmx");
      ObjectName objectName =  new ObjectName(DOMAIN_ALERT+":alert="+bean.getSensorName());
      register(bean, objectName);
    } 
    catch (Exception e)
    {
      _logger.error("", e);
      e.printStackTrace();
    }
  }
  
  public void setAlerts(String originAlert, Map<String, AlertValueAndStatus> alertResultMap, String clusterName)
  {
    if(alertResultMap == null)
    {
      _logger.warn("null alertResultMap");
      return;
    }
    for(String alertName : alertResultMap.keySet())
    {
      String beanName = "";
      if(alertName.length() > 1)
      {
        String comparator = AlertParser.getComponent(AlertParser.COMPARATOR_NAME, originAlert);
        String constant = AlertParser.getComponent(AlertParser.CONSTANT_NAME, originAlert);
        beanName = "("+ alertName+")"+"GREATER("+constant+")";
      }
      else
      {
        beanName = originAlert + "--(" + alertName + ")";
      }
      // This is to make JMX happy; certain charaters cannot be in JMX bean name
      beanName = beanName.replace('*', '%').replace('=', '#').replace(',', ';');
      if(!_alertBeans.containsKey(beanName))
      {
        ClusterAlertItem item = new ClusterAlertItem(beanName, alertResultMap.get(alertName));
        onNewAlertMbeanAdded(item);
        _alertBeans.put(beanName, item);
      }
      else
      {
        _alertBeans.get(beanName).setValueMap(alertResultMap.get(alertName));
      }
    }
    refreshSummayAlert(clusterName);
  }
  /**
   *  The summary alert is a combination of all alerts, if it is on, something is wrong on this 
   *  cluster. The additional info contains all alert mbean names that has been fired.
   */
  void refreshSummayAlert(String clusterName)
  {
    boolean fired = false;
    String alertsFired = "";
    String summaryKey = ALERT_SUMMARY + "_" + clusterName;
    for(String key : _alertBeans.keySet())
    {
      if(!key.equals(summaryKey))
      {
        ClusterAlertItem item = _alertBeans.get(key);
        fired = (item.getAlertFired() == 1) | fired;
        if(item.getAlertFired() == 1)
        {
          alertsFired += item._alertItemName;
          alertsFired += ";";
        }
      }
    }
    Tuple<String> t = new Tuple<String>();
    t.add("0");
    AlertValueAndStatus summaryStatus = new AlertValueAndStatus(t, fired);
    if(!_alertBeans.containsKey(summaryKey))
    {
      ClusterAlertItem item = new ClusterAlertItem(summaryKey, summaryStatus);
      onNewAlertMbeanAdded(item);
      item.setAdditionalInfo(alertsFired);
      _alertBeans.put(summaryKey, item);
    }
    else
    {
      _alertBeans.get(summaryKey).setValueMap(summaryStatus);
      _alertBeans.get(summaryKey).setAdditionalInfo(alertsFired);
    }
  }
  
  void register(Object bean, ObjectName name)
  {
    try
    {
      _beanServer.unregisterMBean(name);
    }
    catch (Exception e)
    {
    }
    try
    {
      _beanServer.registerMBean(bean, name);
    }
    catch (Exception e)
    {
      _logger.error("Could not register MBean", e);
    }
  }
  
  public void reset()
  {
    for(String beanName : _alertBeans.keySet())
    {
      ClusterAlertItem item = _alertBeans.get(beanName);
      item.reset();
      try
      {
        ObjectName objectName =  new ObjectName(DOMAIN_ALERT+":alert="+item.getSensorName());
        _beanServer.unregisterMBean(objectName);
      }
      catch (Exception e)
      {
        _logger.warn("", e);
      }
    }
    _alertBeans.clear();
  }
}
