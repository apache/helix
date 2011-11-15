package com.linkedin.clustermanager.monitoring.adaptors;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.RuntimeOperationsException;
import javax.management.modelmbean.DescriptorSupport;
import javax.management.modelmbean.InvalidTargetObjectTypeException;
import javax.management.modelmbean.ModelMBean;
import javax.management.modelmbean.ModelMBeanAttributeInfo;
import javax.management.modelmbean.ModelMBeanConstructorInfo;
import javax.management.modelmbean.ModelMBeanInfoSupport;
import javax.management.modelmbean.ModelMBeanNotificationInfo;
import javax.management.modelmbean.ModelMBeanOperationInfo;
import javax.management.modelmbean.RequiredModelMBean;
import javax.management.Descriptor;

import com.linkedin.clustermanager.monitoring.Metric;
import com.linkedin.clustermanager.monitoring.Sensor;

public class JmxSensorAdaptor
{
  public static void adaptSensor(Sensor<?> sensor, String domain) throws RuntimeOperationsException, MBeanException, InstanceNotFoundException, InvalidTargetObjectTypeException, InstanceAlreadyExistsException, NotCompliantMBeanException, MalformedObjectNameException, NullPointerException
  {
    String sensorType = sensor.getSensorType();
    ModelMBeanAttributeInfo[] mmbeanAttributeInfo;
    ModelMBeanOperationInfo[] mmbeanOperationInfo;
    try
    {
      mmbeanAttributeInfo = buildModelMBeanAttributeInfo(sensor);
      mmbeanOperationInfo = buildModelMBeanOperationInfo(sensor);
    } catch (IntrospectionException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return;
    }
    Descriptor descriptor = new DescriptorSupport();
    descriptor.setField("name", "ContextTags");
    descriptor.setField("descriptorType", "additional");
    for(String key : sensor.getContextTags().getTags().keySet())
    {
      descriptor.setField(key, sensor.getContextTags().getTags().get(key));
    }
    ModelMBeanInfoSupport modelMBeanInfo =  
      new ModelMBeanInfoSupport(  
         sensor.getStat().getClass().getSimpleName(),  
         sensor.getSensorDescription(),  
         mmbeanAttributeInfo,      // attributes  
         new ModelMBeanConstructorInfo[0],      // constructors  
         mmbeanOperationInfo,      // Operations
         new ModelMBeanNotificationInfo[0]      // notifications  
         );  
    ModelMBean modelmbean = new RequiredModelMBean(modelMBeanInfo);
    modelmbean.setManagedResource(  
        sensor.getStat(),  
        "ObjectReference");  
    
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer(); 
    
    String objectNameString = domain+":type="+sensorType;
    for(String key : sensor.getContextTags().getTags().keySet())
    {
      objectNameString = objectNameString + ","+key+"="+ sensor.getContextTags().getTags().get(key);
    }
    ObjectName objectName =  
      new ObjectName(objectNameString);  
    mbeanServer.registerMBean( modelmbean, objectName );  
    
  }
  public static ModelMBeanOperationInfo[] buildModelMBeanOperationInfo(Sensor<?> sensor)  
  { 
    // TODO: expose operations like getPercentileLatency()
    Set<String> metricNames = sensor.getMetricNames();
    ModelMBeanOperationInfo[] result = new ModelMBeanOperationInfo[metricNames.size()]; 
    int n = 0;
    for(String metricName : metricNames)
    {
      Metric metric = sensor.getMetric(metricName);
      ModelMBeanOperationInfo metricOperateInfo = new ModelMBeanOperationInfo(  
          metric.getMethod().getName(),  
          metric.getMetricDescription(),  
          new MBeanParameterInfo[] {},  
          metric.getMethod().getReturnType().getName(),  
          ModelMBeanOperationInfo.INFO );  
      result[n++] = metricOperateInfo;
    }
    
    return result;
  }
  
  public static ModelMBeanAttributeInfo[] buildModelMBeanAttributeInfo(Sensor<?> sensor) throws IntrospectionException  
  { 
    Set<String> metricNames = sensor.getMetricNames();
    ModelMBeanAttributeInfo[] result = new ModelMBeanAttributeInfo[metricNames.size()]; 
    int n = 0;
    for(String metricName : metricNames)
    {
      Metric metric = sensor.getMetric(metricName);
      List<String> descriptors = new ArrayList<String>();
      descriptors.add("name=" + metricName);
      descriptors.add("displayName="+metricName);  
      descriptors.add("descriptorType=attribute");
      descriptors.add("getMethod=" + metric.getMethod().getName());
      
      final Descriptor attrD = new DescriptorSupport(
              descriptors.toArray(new String[descriptors.size()]));
     
      ModelMBeanAttributeInfo metricAttributeInfo = new ModelMBeanAttributeInfo(  
          metricName,  
          metric.getMetricDescription(),  
          metric.getMethod(),
          null,
          attrD
          );  
      result[n++] = metricAttributeInfo;
    }
    
    return result;
  }

}
