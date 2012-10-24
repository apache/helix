/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix.monitoring;

import org.apache.helix.monitoring.StatCollector;
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
