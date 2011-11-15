package com.linkedin.clustermanager.monitoring;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.monitoring.adaptors.JmxReporter;
import com.linkedin.clustermanager.monitoring.annotations.HelixMetric;

public class TestSensorRegistry
{
  public static class TestSensor extends DataCollector
  {
    StatCollector _collector = new StatCollector();

    @Override
    void notifyDataSample(Object dataSample)
    {
      int value = (Integer)dataSample;
      
      _collector.addData(value);
    }
    
    @HelixMetric(description = "Get mean")
    public double getMean()
    {
      return _collector.getMean();
    }
    
    @HelixMetric(description = "Get data points")
    public long getNumDatapoints()
    {
      return _collector.getNumDataPoints();
    }
    
    @HelixMetric(description = "Get total")
    public double getTotal()
    {
      return _collector.getTotalSum();
    }
  }
  
  @Test(groups={ "unitTest" })
  public void TestContextTags()
  {
    SensorContextTags tag1 = SensorContextTags.fromString("DB=COMM,partition=44,HTTPTYPE=PUT,instance=localhost_22");
    SensorContextTags tag2 = SensorContextTags.fromString("DB=COMM,partition=44,HTTPTYPE=PUT,instance=localhost_24");
    SensorContextTags tag3 = SensorContextTags.fromString("DB=COMM,partition=44,HTTPTYPE=PUT,instance=localhost_25");
    SensorContextTags tag4 = SensorContextTags.fromString("DB=COMM,partition=44,HTTPTYPE=PUT,instance=localhost_22");
    
    Assert.assertTrue(tag1.compareTo(tag2)< 0);
    Assert.assertTrue(tag1.compareTo(tag1) == 0);
    Assert.assertTrue(tag3.compareTo(tag1) > 0);
    Assert.assertTrue(tag4.compareTo(tag1) == 0);
    
    SensorRegistry<TestSensor> _registry = new SensorRegistry<TestSensor>(TestSensor.class);
    JmxReporter reporter = new JmxReporter("com.test.local");
    _registry.addListener(reporter);
    
    SensorTagFilter filter1 = SensorTagFilter.fromString("DB=COMM,partition=*,HTTPTYPE=*,instance=localhost_22");
    SensorTagFilter filter2 = SensorTagFilter.fromString("DB=COMM,partition=*,HTTPTYPE=GET,instance=localhost_23");
    SensorTagFilter filter3 = SensorTagFilter.fromString("DB=COMM,partition=*,HTTPTYPE=*,instance=localhost_24");
    SensorTagFilter filter4 = SensorTagFilter.fromString("DB=COMM,partition=*,HTTPTYPE=*,instance=localhost_25");
    SensorTagFilter filter5 = SensorTagFilter.fromString("DB=COMM,partition=*,HTTPTYPE=*,instance=localhost_23");
    SensorTagFilter filter6 = SensorTagFilter.fromString("DB=COMM");
    SensorTagFilter filter7 = SensorTagFilter.fromString("DB=COMM,partition=44");
    
    _registry.addFilter(filter1);
    _registry.addFilter(filter2);
    _registry.addFilter(filter3);
    _registry.addFilter(filter4);
    _registry.addFilter(filter5);
    _registry.addFilter(filter6);
    _registry.addFilter(filter7);
    
    
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=44,HTTPTYPE=PUT,instance=localhost_22"), 10);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=12,HTTPTYPE=GET,instance=localhost_25"), 20);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=4,HTTPTYPE=GET,instance=localhost_23"), 30);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=33,HTTPTYPE=DELETE,instance=localhost_24"), 10);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=40,HTTPTYPE=PUT,instance=localhost_22"), 20);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=48,HTTPTYPE=GET,instance=localhost_23"), 30);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=18,HTTPTYPE=POST,instance=localhost_25"), 10);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=44,HTTPTYPE=POST,instance=localhost_25"), 20);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=44,HTTPTYPE=DELETE,instance=localhost_25"), 50);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=44,HTTPTYPE=GET,instance=localhost_25"), 100);
    _registry.notifyDataSample(SensorContextTags.fromString("DB=COMM,partition=12,HTTPTYPE=POST,instance=localhost_25"), 200);
    
    Assert.assertTrue(_registry._sensors.size() == 13);
    Assert.assertTrue((int)(_registry._sensors.get(SensorContextTags.fromString("DB=COMM")).getStat().getTotal()) == 500);
    Assert.assertTrue((int)(_registry._sensors.get(SensorContextTags.fromString("DB=COMM")).getStat().getNumDatapoints()) == 11);
    Assert.assertTrue((int)(_registry._sensors.get(SensorContextTags.fromString("DB=COMM,partition=44")).getStat().getTotal()) == 180);
    Assert.assertTrue((int)(_registry._sensors.get(SensorContextTags.fromString("DB=COMM,partition=44")).getStat().getNumDatapoints()) == 4);
    
  }
}
