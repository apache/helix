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


public class ClusterStatusMonitor
  implements ClusterStatusMonitorMBean,LiveInstanceChangeListener, ExternalViewChangeListener
{
  private static final Logger LOG = Logger.getLogger(ClusterStatusMonitor.class);

  private final MBeanServer _beanServer;

  private int _numOfLiveInstances;
  private final int _numOfInstances;
  private final ConcurrentHashMap<String, ResourceMonitor> _resourceMbeanMap
    = new ConcurrentHashMap<String, ResourceMonitor>();
  private String _clusterName = "";

  private final List<ResourceMonitorChangedListener> _listeners = new ArrayList<ResourceMonitorChangedListener>() ;

  public void addResourceMonitorChangedListener(ResourceMonitorChangedListener listener)
  {
    synchronized(_listeners)
    {
      if(!_listeners.contains(listener))
      {
        _listeners.add(listener);
        for(ResourceMonitor bean : _resourceMbeanMap.values())
        {
          listener.onResourceMonitorAdded(bean);
        }
      }
    }
  }

  private void notifyListeners(ResourceMonitor newResourceMonitor)
  {
    synchronized(_listeners)
    {
      for(ResourceMonitorChangedListener listener : _listeners)
      {
        listener.onResourceMonitorAdded(newResourceMonitor);
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
              notifyListeners(bean);
            }
          }
        }
        _resourceMbeanMap.get(resourceName).onExternalViewChange(externalView, changeContext.getManager());
      }
    }
    catch(Exception e)
    {
      LOG.warn(e);
    }

  }
}
