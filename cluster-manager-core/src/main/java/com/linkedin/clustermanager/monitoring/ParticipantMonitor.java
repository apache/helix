package com.linkedin.clustermanager.monitoring;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.monitoring.mbeans.ResourceGroupMonitor;
import com.linkedin.clustermanager.monitoring.mbeans.ResourceGroupMonitorChangedListener;
import com.linkedin.clustermanager.monitoring.mbeans.StateTransitionStatMonitor;

public class ParticipantMonitor
{
  private ConcurrentHashMap<StateTransitionContext, StateTransitionStatMonitor> _monitorMap
   = new ConcurrentHashMap<StateTransitionContext, StateTransitionStatMonitor>();
  private static final Logger LOG = Logger.getLogger(ParticipantMonitor.class);

  private MBeanServer _beanServer;
  
  private List<TransStatMonitorChangedListener> _listeners = new ArrayList<TransStatMonitorChangedListener>() ;
  
  public void addTransStatMonitorChangedListener(TransStatMonitorChangedListener listener)
  {
    synchronized(_listeners)
    {
      if(!_listeners.contains(listener))
      {
        _listeners.add(listener);
        for(StateTransitionStatMonitor bean : _monitorMap.values())
        {
          listener.onTransStatMonitorAdded(bean);
        }
      }
    }
  }
  
  private void notifyListeners(StateTransitionStatMonitor newStateTransitionStatMonitor)
  {
    synchronized(_listeners)
    {
      for(TransStatMonitorChangedListener listener : _listeners)
      {
        listener.onTransStatMonitorAdded(newStateTransitionStatMonitor);
      }
    }
  }
  
  public ParticipantMonitor()
  {
    try
    {
      _beanServer = ManagementFactory.getPlatformMBeanServer();
    }
    catch(Exception e)
    {
      LOG.warn(e);
      e.printStackTrace();
      _beanServer = null;
    }
  }

  public void reportTransitionStat(StateTransitionContext cxt,
      StateTransitionDataPoint data)
  {
    if(_beanServer == null)
    {
      LOG.warn("bean server is null, skip reporting");
      return;
    }
    try
    {
      if(!_monitorMap.containsKey(cxt))
      {
        synchronized(this)
        {
          if(!_monitorMap.containsKey(cxt))
          {
            StateTransitionStatMonitor bean = new StateTransitionStatMonitor(cxt, TimeUnit.MILLISECONDS);
            _monitorMap.put(cxt, bean);
            String beanName = cxt.toString();
            register(bean, getObjectName(beanName));
            notifyListeners(bean);
          }
        }
      }
      _monitorMap.get(cxt).addDataPoint(data);
    }
    catch(Exception e)
    {
      LOG.warn(e);
      e.printStackTrace();
    }
  }
  

  private ObjectName getObjectName(String name) throws MalformedObjectNameException
  {
    return new ObjectName("CLMParticipantReport:"+name);
  }
  
  private void register(Object bean, ObjectName name)
  {
    if(_beanServer == null)
    {
      LOG.warn("bean server is null, skip reporting");
      return;
    }
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
}
