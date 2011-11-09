package com.linkedin.clustermanager.monitoring;

public class Sensor<T>
{
   final  T _stat;
   final SensorContextTags _tags;
   
   public Sensor(T stat, SensorContextTags tags)
   {
     _stat = stat;
     _tags = tags;
   }
   
   public T getStat()
   {
     return _stat;
   }
   
   public SensorContextTags getContextTags()
   {
     return _tags;
   }
}
