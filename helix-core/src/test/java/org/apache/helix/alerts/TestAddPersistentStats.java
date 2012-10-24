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
package org.apache.helix.alerts;

import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.Mocks.MockManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.alerts.StatsHolder;
import org.apache.helix.controller.stages.HealthDataCache;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestAddPersistentStats
{

  protected static final String CLUSTER_NAME = "TestCluster";

  MockManager                   _helixManager;
  StatsHolder                   _statsHolder;

  @BeforeMethod(groups = { "unitTest" })
  public void setup()
  {
    _helixManager = new MockManager(CLUSTER_NAME);
    _statsHolder = new StatsHolder(_helixManager, new HealthDataCache());
  }

  public boolean statRecordContains(ZNRecord rec, String statName)
  {
    Map<String, Map<String, String>> stats = rec.getMapFields();
    return stats.containsKey(statName);
  }

  public int statsSize(ZNRecord rec)
  {
    Map<String, Map<String, String>> stats = rec.getMapFields();
    return stats.size();
  }

  @Test(groups = { "unitTest" })
  public void testAddStat() throws Exception
  {
    String stat = "window(5)(dbFoo.partition10.latency)";
    _statsHolder.addStat(stat);
    _statsHolder.persistStats();

    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();
    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordContains(rec, stat));
    AssertJUnit.assertEquals(1, statsSize(rec));
  }

  @Test(groups = { "unitTest" })
  public void testAddTwoStats() throws Exception
  {
    String stat1 = "window(5)(dbFoo.partition10.latency)";
    _statsHolder.addStat(stat1);
    _statsHolder.persistStats();
    String stat2 = "window(5)(dbFoo.partition11.latency)";
    _statsHolder.addStat(stat2);
    _statsHolder.persistStats();

    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordContains(rec, stat1));
    AssertJUnit.assertTrue(statRecordContains(rec, stat2));
    AssertJUnit.assertEquals(2, statsSize(rec));
  }

  @Test(groups = { "unitTest" })
  public void testAddDuplicateStat() throws Exception
  {
    String stat = "window(5)(dbFoo.partition10.latency)";
    _statsHolder.addStat(stat);
    _statsHolder.addStat(stat);
    _statsHolder.persistStats();

    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordContains(rec, stat));
    AssertJUnit.assertEquals(1, statsSize(rec));
  }

  @Test(groups = { "unitTest" })
  public void testAddPairOfStats() throws Exception
  {
    String exp = "accumulate()(dbFoo.partition10.latency, dbFoo.partition10.count)";
    _statsHolder.addStat(exp);
    _statsHolder.persistStats();

    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();
    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordContains(rec,
                                              "accumulate()(dbFoo.partition10.latency)"));
    AssertJUnit.assertTrue(statRecordContains(rec,
                                              "accumulate()(dbFoo.partition10.count)"));
    AssertJUnit.assertEquals(2, statsSize(rec));
  }

  @Test(groups = { "unitTest" })
  public void testAddStatsWithOperators() throws Exception
  {
    String exp =
        "accumulate()(dbFoo.partition10.latency, dbFoo.partition10.count)|EACH|ACCUMULATE|DIVIDE";
    _statsHolder.addStat(exp);
    _statsHolder.persistStats();

    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordContains(rec,
                                              "accumulate()(dbFoo.partition10.latency)"));
    AssertJUnit.assertTrue(statRecordContains(rec,
                                              "accumulate()(dbFoo.partition10.count)"));
    AssertJUnit.assertEquals(2, statsSize(rec));
  }

  @Test(groups = { "unitTest" })
  public void testAddNonExistentAggregator() throws Exception
  {
    String exp = "fakeagg()(dbFoo.partition10.latency)";
    boolean caughtException = false;
    try
    {
      _statsHolder.addStat(exp);
    }
    catch (HelixException e)
    {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test(groups = { "unitTest" })
  public void testGoodAggregatorBadArgs() throws Exception
  {
    String exp = "accumulate(10)(dbFoo.partition10.latency)";
    boolean caughtException = false;
    try
    {
      _statsHolder.addStat(exp);
    }
    catch (HelixException e)
    {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test(groups = { "unitTest" })
  public void testAddBadNestingStat1() throws Exception
  {
    String exp = "window((5)(dbFoo.partition10.latency)";
    boolean caughtException = false;
    try
    {
      _statsHolder.addStat(exp);
    }
    catch (HelixException e)
    {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test(groups = { "unitTest" })
  public void testAddBadNestingStat2() throws Exception
  {
    String exp = "window(5)(dbFoo.partition10.latency))";
    boolean caughtException = false;
    try
    {
      _statsHolder.addStat(exp);
    }
    catch (HelixException e)
    {
      caughtException = true;
    }
    AssertJUnit.assertTrue(caughtException);
  }
}
