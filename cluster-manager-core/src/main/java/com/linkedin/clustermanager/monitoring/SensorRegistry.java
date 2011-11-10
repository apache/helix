package com.linkedin.clustermanager.monitoring;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.log4j.Logger;

public class SensorRegistry<T extends DataCollector>
{
   private final Class<T> _clazz;
   static Logger _logger = Logger.getLogger(SensorRegistry.class);
   final ConcurrentHashMap<SensorContextTags, Sensor<T>> _sensors 
     = new ConcurrentHashMap<SensorContextTags, Sensor<T>>();
   final Set<TagFilter> _filters = new ConcurrentSkipListSet<TagFilter>();
   final Set<SensorRegistryListener> _listeners = new ConcurrentSkipListSet<SensorRegistryListener>();
   
   public SensorRegistry(Class<T> clazz)
   {
     _clazz = clazz;
   }
   
   public void addListener(SensorRegistryListener listener)
   {
     if(!_listeners.contains(listener))
     {
       _listeners.add(listener);
       List<TagFilter> filterList = listener.getContextTagFilterList();
       for(SensorContextTags tag : _sensors.keySet())
       {
         for(TagFilter filter : filterList)
         {
           if(filter.matchs(tag))
           {
             listener.onSensorAdded(_sensors.get(tag));
             break;
           }
         }
       }
     }
   }
   
   public void removeListener(SensorRegistryListener listener)
   {
     if(_listeners.contains(listener))
     {
       _listeners.remove(listener);
     }
   }
   
   public void addFilter(TagFilter filter)
   {
     if(!_filters.contains(filter))
     {
       _filters.add(filter);
     }
   }
   
   
   public void removeFilter(TagFilter filter)
   {
     _filters.remove(filter);
   }
   
   public Map<SensorContextTags, Sensor<T>> getMatchedSensors(SensorContextTags tags)
   {
     Map<SensorContextTags, Sensor<T>> result = new HashMap<SensorContextTags, Sensor<T>>();
     for(TagFilter filter : _filters)
     {
       SensorContextTags filteredTags = filter.getFilteredTags(tags);
       if(!_sensors.containsKey(filteredTags))
       {
         // Create a sensor if not exist
         try
         {
           _sensors.put(filteredTags, new Sensor<T>(_clazz.newInstance(), filteredTags));
           for(SensorRegistryListener listener : _listeners)
           {
             listener.onSensorAdded(_sensors.get(filteredTags));
           }
         } 
         catch (InstantiationException e)
         {
           e.printStackTrace();
           _logger.warn("error", e);
         } 
         catch (IllegalAccessException e)
         {
           e.printStackTrace();
           _logger.warn("error", e);
         }
       }
       result.put(filteredTags, _sensors.get(filteredTags));
     }
     return result;
   }
   
   public void notifyDataSample(SensorContextTags tags, Object dataSample)
   {
     Map<SensorContextTags, Sensor<T>> matchedSensors = getMatchedSensors(tags);
     for(Sensor<T> sensor : matchedSensors.values())
     {
       sensor.getStat().notifyDataSample(dataSample);
     }
   }
}
