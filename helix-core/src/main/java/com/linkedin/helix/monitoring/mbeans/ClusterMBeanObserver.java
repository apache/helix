package com.linkedin.helix.monitoring.mbeans;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ReflectionException;
import javax.management.relation.MBeanServerNotificationFilter;

import org.apache.log4j.Logger;

/*
 * TODO: this class should be in espresso common, as the only usage of it is
 * to create ingraph adaptors
 * **/
public abstract class ClusterMBeanObserver implements NotificationListener
{
  protected final String _domain;
  protected MBeanServerConnection _server;
  private static final Logger _logger = Logger.getLogger(ClusterMBeanObserver.class);
      
  public ClusterMBeanObserver(String domain) 
      throws InstanceNotFoundException, IOException, MalformedObjectNameException, NullPointerException
  {
    // Get a reference to the target MBeanServer
    _domain = domain;
    _server = ManagementFactory.getPlatformMBeanServer();
    MBeanServerNotificationFilter filter = new MBeanServerNotificationFilter();
    filter.enableAllObjectNames();
    _server.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this, filter, null);
  }
  
  public void handleNotification(Notification notification, Object handback)
  {
    MBeanServerNotification mbs = (MBeanServerNotification) notification;
    if(MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(mbs.getType())) 
    {
      if(mbs.getMBeanName().getDomain().equalsIgnoreCase(_domain))
      {
        _logger.info("MBean Registered, name :" + mbs.getMBeanName());
        onMBeanRegistered(_server, mbs);
      } 
    }
    else if(MBeanServerNotification.UNREGISTRATION_NOTIFICATION.equals(mbs.getType())) 
    {
      if(mbs.getMBeanName().getDomain().equalsIgnoreCase(_domain))
      {
        _logger.info("MBean Unregistered, name :" + mbs.getMBeanName());
        onMBeanUnRegistered(_server, mbs);
      }
    }
  }
  
  public abstract void onMBeanRegistered(MBeanServerConnection server, MBeanServerNotification mbsNotification);
  
  public abstract void onMBeanUnRegistered(MBeanServerConnection server, MBeanServerNotification mbsNotification);  
  
}
