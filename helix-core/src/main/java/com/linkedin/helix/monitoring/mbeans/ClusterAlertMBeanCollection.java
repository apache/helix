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

public class ClusterAlertMBeanCollection
{
  public static String DOMAIN_ALERT = "HelixAlerts";
  
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
      _logger.info("alert bean " + bean.getAlertName()+" exposed to jmx");
      ObjectName objectName =  new ObjectName(DOMAIN_ALERT+":alert="+bean.getAlertName());
      register(bean, objectName);
      
      // for testing only
      // ObjectName objectName2 =  new ObjectName(DOMAIN_ALERT+".clone:alert="+bean.getAlertName());
      // register(bean, objectName2);
    } 
    catch (Exception e)
    {
      _logger.error("", e);
    }
  }
  
  public void setAlerts(Map<String, Integer> alertResultMap)
  {
    for(String alertName : alertResultMap.keySet())
    {
      if(!_alertBeans.containsKey(alertName))
      {
        ClusterAlertItem item = new ClusterAlertItem(alertName, alertResultMap.get(alertName));
        onNewAlertMbeanAdded(item);
        _alertBeans.put(alertName, item);
      }
      else
      {
        _alertBeans.get(alertName).setValue(alertResultMap.get(alertName));
      }
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
      _logger.warn("Could not register MBean", e);
    }
  }
}
