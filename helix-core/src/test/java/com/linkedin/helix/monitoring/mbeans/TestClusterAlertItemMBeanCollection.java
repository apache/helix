package com.linkedin.helix.monitoring.mbeans;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.alerts.AlertValueAndStatus;
import com.linkedin.helix.monitoring.mbeans.ClusterAlertMBeanCollection;
import com.linkedin.helix.monitoring.mbeans.ClusterMBeanObserver;

public class TestClusterAlertItemMBeanCollection
{
  private static final Logger _logger = Logger.getLogger(TestClusterAlertItemMBeanCollection.class);
  
  class TestClusterMBeanObserver extends ClusterMBeanObserver
  {
    Map<String, Map<String, Object>> _beanValueMap = new HashMap<String, Map<String, Object>>();
    
    public TestClusterMBeanObserver(String domain)
        throws InstanceNotFoundException, IOException,
        MalformedObjectNameException, NullPointerException
    {
      super(domain);
    }

    @Override
    public void onMBeanRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification)
    {
      try
      {
        MBeanInfo info = _server.getMBeanInfo(mbsNotification.getMBeanName());
        MBeanAttributeInfo[] infos = info.getAttributes();
        _beanValueMap.put(mbsNotification.getMBeanName().toString(), new HashMap<String, Object>());
        for(MBeanAttributeInfo infoItem : infos)
        {
          Object val = _server.getAttribute(mbsNotification.getMBeanName(), infoItem.getName());
          System.out.println("         " + infoItem.getName() + " : " + _server.getAttribute(mbsNotification.getMBeanName(), infoItem.getName()) + " type : " + infoItem.getType());
          _beanValueMap.get(mbsNotification.getMBeanName().toString()).put(infoItem.getName(), val);
        }
      } 
      catch (Exception e)
      {
        _logger.error("Error getting bean info, domain="+_domain, e);
      } 
    }

    @Override
    public void onMBeanUnRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification)
    {
      
    } 
  }
}
