package com.linkedin.clustermanager.monitoring;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
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
    AssertJUnit.assertEquals(collector.getNumDataPoints(), nPoints);
    AssertJUnit.assertEquals((long)collector.getMax(), 99000);
    AssertJUnit.assertEquals((long)collector.getTotalSum(), 4950000);
    AssertJUnit.assertEquals((long)collector.getPercentile(40), 39400);
    AssertJUnit.assertEquals((long)collector.getMean(), 49500);
    AssertJUnit.assertEquals((long)collector.getMin(), 0);
    
    collector.reset();
    
    AssertJUnit.assertEquals(collector.getNumDataPoints(), 0);
    AssertJUnit.assertEquals((long)collector.getMax(), 0);
    AssertJUnit.assertEquals((long)collector.getTotalSum(), 0);
    AssertJUnit.assertEquals((long)collector.getPercentile(40), 0);
    AssertJUnit.assertEquals((long)collector.getMean(), 0);
    AssertJUnit.assertEquals((long)collector.getMin(), 0);
    
  }
}
