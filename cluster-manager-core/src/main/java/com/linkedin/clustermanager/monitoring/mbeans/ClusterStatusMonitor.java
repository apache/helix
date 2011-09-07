package com.linkedin.clustermanager.monitoring.mbeans;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;


public class ClusterStatusMonitor 
  implements ClusterStatusMonitorMBean,LiveInstanceChangeListener, ExternalViewChangeListener
{
  private static final Logger LOG = Logger.getLogger(ClusterStatusMonitor.class);

  private final MBeanServer _beanServer;
  
  private int _numOfLiveInstances;
  private int _numOfInstances;
  private ConcurrentHashMap<String, ResourceGroupMonitor> _resourceGroupMbeanMap 
    = new ConcurrentHashMap<String, ResourceGroupMonitor>();
  private String _clusterName = "";
  
  public ClusterStatusMonitor(String clusterName, int nInstances)
  {
    _clusterName = clusterName;
    _numOfInstances = nInstances;
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    try
    {
      register(this, getObjectName("cluster="+_clusterName));
    }
    catch(Exception e)
    {
      LOG.error("register self failed.", e);
      e.printStackTrace();
    }
  }
  
  private ObjectName getObjectName(String name) throws MalformedObjectNameException
  {
    return new ObjectName("CLMController:"+name);
  }
  
  @Override
  public long getNumberOfLiveInstances()
  {
    return _numOfLiveInstances;
  }

  @Override
  public long getNumberOfInstances()
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

  @Override
  public void onLiveInstanceChange(List<ZNRecord> liveInstances,
      NotificationContext changeContext)
  {
    try
    {
      _numOfLiveInstances = liveInstances.size();
    }
    catch(Exception e)
    {
      LOG.warn(e);
      //e.printStackTrace();
    }
  }

  @Override
  public void onExternalViewChange(List<ZNRecord> externalViewList,
      NotificationContext changeContext)
  {
    try
    {
      for(ZNRecord externalView : externalViewList)
      {
        String resourceGroup = externalView.getId();
        if(!_resourceGroupMbeanMap.containsKey(resourceGroup))
        {
          synchronized(this)
          {
            if(!_resourceGroupMbeanMap.containsKey(resourceGroup))
            {
              ResourceGroupMonitor bean = new ResourceGroupMonitor();
              String beanName = "Cluster=" + _clusterName + ",resourceGroup=" + resourceGroup;
              register(bean, getObjectName(beanName));
              _resourceGroupMbeanMap.put(resourceGroup, bean);
            }
          }
        }
        _resourceGroupMbeanMap.get(resourceGroup).onExternalViewChange(externalView, changeContext.getManager());
      }
    }
    catch(Exception e)
    {
      LOG.warn(e);
      //e.printStackTrace();
    }
    
  }
}
