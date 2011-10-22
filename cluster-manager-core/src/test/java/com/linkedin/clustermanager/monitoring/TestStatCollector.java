package com.linkedin.clustermanager.monitoring;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestStatCollector
{
  @Test(groups={ "unitTest" })
  public void TestCollectData()
  {
    StatCollector collector = new StatCollector();
    
    int nPoints = 100;
    for (int i = 0; i< nPoints; i++)
    {
      collector.addData(i*1000);
    }
    Assert.assertEquals(collector.getNumDataPoints(), nPoints);
    Assert.assertEquals((long)collector.getMax(), 99000);
    Assert.assertEquals((long)collector.getTotalSum(), 4950000);
    Assert.assertEquals((long)collector.getPercentile(40), 39400);
    Assert.assertEquals((long)collector.getMean(), 49500);
    Assert.assertEquals((long)collector.getMin(), 0);
    
    collector.reset();
    
    Assert.assertEquals(collector.getNumDataPoints(), 0);
    Assert.assertEquals((long)collector.getMax(), 0);
    Assert.assertEquals((long)collector.getTotalSum(), 0);
    Assert.assertEquals((long)collector.getPercentile(40), 0);
    Assert.assertEquals((long)collector.getMean(), 0);
    Assert.assertEquals((long)collector.getMin(), 0);
    
  }
}
