package com.linkedin.helix.monitoring.mbeans;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.LiveInstance;


public class ClusterStatusMonitor
  implements ClusterStatusMonitorMBean
{
  private static final Logger LOG = Logger.getLogger(ClusterStatusMonitor.class);

  private final MBeanServer _beanServer;

  private int _numOfLiveInstances = 0;
  private int _numOfInstances = 0;
  private final ConcurrentHashMap<String, ResourceMonitor> _resourceMbeanMap
    = new ConcurrentHashMap<String, ResourceMonitor>();
  private String _clusterName = "";

  public ClusterStatusMonitor(String clusterName)
  {
    _clusterName = clusterName;
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    try
    {
      register(this, getObjectName("cluster="+_clusterName));
    }
    catch(Exception e)
    {
      LOG.error("register self failed.", e);
    }
  }

  public ObjectName getObjectName(String name) throws MalformedObjectNameException
  {
    return new ObjectName("ClusterStatus: "+name);
  }

  // Used by other external JMX consumers like ingraph
  public String getBeanName()
  {
    return "ClusterStatus "+_clusterName;
  }

  @Override
  public long getDownInstanceGauge()
  {
    return _numOfInstances - _numOfLiveInstances;
  }

  @Override
  public long getInstancesGauge()
  {
    return _numOfInstances;
  }

  private void register(Object bean, ObjectName name)
  {
    try
    {
      _beanServer.unregisterMBean(name);
    }
    catch (Exception e1)
    {
      // Swallow silently
    }

    try
    {
      _beanServer.registerMBean(bean, name);
    }
    catch (Exception e)
    {
      LOG.warn("Could not register MBean", e);
    }
  }
  
  private void unregister(ObjectName name)
  {
    try
    {
      _beanServer.unregisterMBean(name);
    }
    catch (Exception e)
    {
      //LOG.warn("Could not unregister MBean", e);
    }
  }

  public void setLiveInstanceNum(int numberLiveInstances, int numberOfInstances)
  {
    _numOfInstances = numberOfInstances;
    _numOfLiveInstances = numberLiveInstances;
  }

  public void onExternalViewChange(ExternalView externalView, IdealState idealState)
  {
    try
    {
        String resourceName = externalView.getId();
        if(!_resourceMbeanMap.containsKey(resourceName))
        {
          synchronized(this)
          {
            if(!_resourceMbeanMap.containsKey(resourceName))
            {
              ResourceMonitor bean = new ResourceMonitor(_clusterName, resourceName);
              String beanName = "Cluster=" + _clusterName + ",resourceName=" + resourceName;
              register(bean, getObjectName(beanName));
              _resourceMbeanMap.put(resourceName, bean);
            }
          }
        }
        _resourceMbeanMap.get(resourceName).updateExternalView(externalView, idealState);
    }
    catch(Exception e)
    {
      LOG.warn(e);
    }
  }

  public void reset()
  {
    LOG.info("Resetting ClusterStatusMonitor");
    try
    {
      for(String resourceName : _resourceMbeanMap.keySet())
      {
        String beanName = "Cluster=" + _clusterName + ",resourceName=" + resourceName;
        unregister(getObjectName(beanName));
      }
      _resourceMbeanMap.clear();
      unregister(getObjectName("cluster="+_clusterName));
    }
    catch(Exception e)
    {
      LOG.error("register self failed.", e);
    }
  }
  
  public String getSensorName()
  {
    return "ClusterStatus";
  }
}
