package com.linkedin.clustermanager.monitoring;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.linkedin.clustermanager.monitoring.annotations.HelixSensorAttribute;

public class Sensor<T>
{
   final  T _stat;
   final SensorContextTags _tags;
   final Map<String, Metric> _sensorMetrics = new HashMap<String, Metric>();
   
   public Sensor(T stat, SensorContextTags tags)
   {
     _stat = stat;
     _tags = tags;
     initSensorAttributes();
   }
   
   public T getStat()
   {
     return _stat;
   }
   
   public SensorContextTags getContextTags()
   {
     return _tags;
   }
   
   void initSensorAttributes()
   {
     Method[] methods = _stat.getClass().getMethods();
     for(Method method : methods)
     {
       if(method.getAnnotation(HelixSensorAttribute.class) != null)
       {
         if(method.getParameterTypes().length == 0)
         {
           String methodName = method.getName();
           if(methodName.startsWith("get"))
           {
             methodName = methodName.substring(3);
           }
           Metric metric = new Metric( _stat, method, methodName);
           _sensorMetrics.put(methodName, metric);
         }
       }
     }
   }
   
   public Set<String> getMetricNames()
   {
     return _sensorMetrics.keySet();
   }
   
   public Metric getMetric(String metricName)
   {
     return _sensorMetrics.get(metricName);
   }
}
