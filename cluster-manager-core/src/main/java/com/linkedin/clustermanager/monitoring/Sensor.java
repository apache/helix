package com.linkedin.clustermanager.monitoring;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.linkedin.clustermanager.monitoring.annotations.HelixMetric;
import com.linkedin.clustermanager.monitoring.annotations.HelixSensor;

public class Sensor<T>
{
   final  T _stat;
   final SensorContextTags _tags;
   final Map<String, Metric> _sensorMetrics = new HashMap<String, Metric>();
   final String _sensorType;
   final String _sensorDescription;
   public Sensor(T stat, SensorContextTags tags)
   {
     _stat = stat;
     _tags = tags;
     initSensorAttributes();
     String sensorType = null;
     String sensorDescription = null;
     if(_stat.getClass().getAnnotation(HelixSensor.class)!=null)
     {
       sensorType = _stat.getClass().getAnnotation(HelixSensor.class).sensorType();
       sensorDescription = _stat.getClass().getAnnotation(HelixSensor.class).description();
     }
     else
     {
       sensorType = _stat.getClass().getSimpleName();
       sensorDescription = _stat.getClass().getName();
     }
     _sensorType = sensorType;
     _sensorDescription = sensorDescription;
   }
   
   public T getStat()
   {
     return _stat;
   }
   
   public String getSensorType()
   {
     return _sensorType;
   }
   
   public String getSensorDescription()
   {
     return _sensorDescription;
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
       if(method.getAnnotation(HelixMetric.class) != null)
       {
         if(method.getParameterTypes().length == 0)
         {
           String methodName = method.getName();
           if(methodName.startsWith("get"))
           {
             methodName = methodName.substring(3);
           }
           Metric metric = new Metric( _stat, method, methodName, method.getAnnotation(HelixMetric.class).description());
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
