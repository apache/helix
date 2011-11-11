package com.linkedin.clustermanager.monitoring;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Metric
{
  String _metricName;
  Method _method;
  Object _sensor;
  MetricType _metricType;
  
  public Metric(Object sensor, Method method, String name)
  {
    _metricName = name;
    _method = method;
    _sensor = sensor;
    if(_metricName.toLowerCase().indexOf("counter")!=-1)
    {
      _metricType = MetricType.COUNTER;
    }
    else
    {
      _metricType = MetricType.GAUGE;
    }
  }
  
  public String getMetricName()
  {
    return _metricName;
  }
  
  public MetricType getMetricType()
  {
    return _metricType;
  }
  
  public String getMetricValue()
  {
    try
    {
      Object result = _method.invoke(_sensor);
      return result.toString();
    } 
    catch (IllegalArgumentException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InvocationTargetException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return "";
  }
  
}
