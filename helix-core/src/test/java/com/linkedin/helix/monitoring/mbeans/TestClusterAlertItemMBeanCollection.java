package com.linkedin.helix.monitoring.mbeans;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

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
  
  @Test (groups = {"unitTest"})
  public void testAlertNotification() throws InstanceNotFoundException, IOException, MalformedObjectNameException, NullPointerException
  {
    ClusterAlertMBeanCollection beanCollection = new ClusterAlertMBeanCollection();
    int nAlerts = 22;
    TestClusterMBeanObserver observer1 = new TestClusterMBeanObserver(ClusterAlertMBeanCollection.DOMAIN_ALERT);
    Map<String, Integer> alertsMap1 = new HashMap<String, Integer>();
    for (int i = 0;i < nAlerts; i++)
    {
      alertsMap1.put("Alert"+i, i%2);
    }
    beanCollection.setAlerts(alertsMap1);
    try
    {
      Thread.sleep(1000);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Assert.assertTrue(observer1._beanValueMap.size() == 22);
    for (int i = 0;i < nAlerts; i++)
    {
      String beanName = "HelixAlerts:alert=Alert"+i;
      Assert.assertTrue(observer1._beanValueMap.containsKey(beanName));
      Assert.assertTrue(observer1._beanValueMap.get(beanName).get("AlertName").equals("Alert"+i));

      Assert.assertTrue(((Integer)(observer1._beanValueMap.get(beanName).get("AlertValue"))).intValue() == i%2);
    }
  }
  
}
