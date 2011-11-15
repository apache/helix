package com.linkedin.clustermanager.monitoring.adaptors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.RuntimeOperationsException;
import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollectorImpl;
import com.linkedin.clustermanager.monitoring.Sensor;
import com.linkedin.clustermanager.monitoring.SensorContextTags;
import com.linkedin.clustermanager.monitoring.SensorRegistryListener;
import com.linkedin.clustermanager.monitoring.SensorTagFilter;

public class JmxReporter implements SensorRegistryListener, Comparable<JmxReporter>
{
  private static final Logger _logger = Logger.getLogger(JmxReporter.class);
  final List<SensorTagFilter> _tagFilterList= new ArrayList<SensorTagFilter>();
  final ConcurrentHashMap<String, Sensor<?>> _exposedSensors = new ConcurrentHashMap<String, Sensor<?>>();
  final String _domain;
  public JmxReporter(String domain)
  {
    _domain = domain;
  }
  
  @Override
  public void onSensorAdded(Sensor<?> sensor)
  {
    boolean shouldExpose = false;
    // expose everything if no filter is specified
    if(_tagFilterList.size() == 0)
    {
      shouldExpose = true;
    }
    else
    {
      shouldExpose = false;
      for(SensorTagFilter filter : _tagFilterList)
      {
        if(filter.matchs(sensor.getContextTags()))
        {
          shouldExpose = true;
          break;
        }
      }
    }
    
    if(shouldExpose)
    {
      String key = sensor.getContextTags().toString()+"-"+sensor.getSensorType();
      if(!_exposedSensors.containsKey(key))
      {
        try
        {
          JmxSensorAdaptor.adaptSensor(sensor, _domain);
          _exposedSensors.put(key, sensor);
        } 
        catch (Exception e)
        {
          _logger.error("", e);
        } 
      }
    }
  }
  
  public String getDomain()
  {
    return _domain;
  }

  @Override
  public List<SensorTagFilter> getContextTagFilterList()
  {
    // TODO Auto-generated method stub
    return _tagFilterList;
  }
  
  public void addTagFilter(SensorTagFilter filter)
  {
    if(!_tagFilterList.contains(filter))
    {
      _tagFilterList.add(filter);
    }
    else
    {
      _logger.warn("TagFilter " + filter + " already added");
    }
  }

  public void removeTagFilter(SensorTagFilter filter)
  {
    if(_tagFilterList.contains(filter))
    {
      _tagFilterList.remove(filter);
    }
    else
    {
      _logger.warn("TagFilter " + filter + " already removed");
    }
  }

  @Override
  public int compareTo(JmxReporter o)
  {
    if(this.equals(o))
    {
      return 0;
    }
    return 1;
  }
}
