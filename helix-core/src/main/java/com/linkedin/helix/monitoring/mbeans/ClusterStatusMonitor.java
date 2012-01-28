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
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.monitoring.annotations.HelixMetric;


public class ClusterStatusMonitor
  implements ClusterStatusMonitorMBean,LiveInstanceChangeListener, ExternalViewChangeListener
{
  private static final Logger LOG = Logger.getLogger(ClusterStatusMonitor.class);

  private final MBeanServer _beanServer;

  private int _numOfLiveInstances;
  private final int _numOfInstances;
  private final ConcurrentHashMap<String, ResourceGroupMonitor> _resourceGroupMbeanMap
    = new ConcurrentHashMap<String, ResourceGroupMonitor>();
  private String _clusterName = "";

  private final List<ResourceGroupMonitorChangedListener> _listeners = new ArrayList<ResourceGroupMonitorChangedListener>() ;

  public void addResourceGroupMonitorChangedListener(ResourceGroupMonitorChangedListener listener)
  {
    synchronized(_listeners)
    {
      if(!_listeners.contains(listener))
      {
        _listeners.add(listener);
        for(ResourceGroupMonitor bean : _resourceGroupMbeanMap.values())
        {
          listener.onResourceGroupMonitorAdded(bean);
        }
      }
    }
  }

  private void notifyListeners(ResourceGroupMonitor newResourceGroupMonitor)
  {
    synchronized(_listeners)
    {
      for(ResourceGroupMonitorChangedListener listener : _listeners)
      {
        listener.onResourceGroupMonitorAdded(newResourceGroupMonitor);
      }
    }
  }

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
  @HelixMetric(description = "Get the total live instance")
  public long getLiveInstanceGauge()
  {
    return _numOfLiveInstances;
  }

  @Override
  @HelixMetric(description = "Get the total instance")
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

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
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
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext)
  {
    try
    {
      for(ExternalView externalView : externalViewList)
      {
        String resourceGroup = externalView.getId();
        if(!_resourceGroupMbeanMap.containsKey(resourceGroup))
        {
          synchronized(this)
          {
            if(!_resourceGroupMbeanMap.containsKey(resourceGroup))
            {
              ResourceGroupMonitor bean = new ResourceGroupMonitor(_clusterName, resourceGroup);
              String beanName = "Cluster=" + _clusterName + ",resourceGroup=" + resourceGroup;
              register(bean, getObjectName(beanName));
              _resourceGroupMbeanMap.put(resourceGroup, bean);
              notifyListeners(bean);
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
