package org.apache.helix.alerts;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.Mocks.MockManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.alerts.StatsHolder;
import org.apache.helix.alerts.Tuple;
import org.apache.helix.controller.stages.HealthDataCache;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestArrivingParticipantStats {
  protected static final String CLUSTER_NAME = "TestCluster";

  MockManager _helixManager;
  StatsHolder _statsHolder;

  @BeforeMethod(groups = {
    "unitTest"
  })
  public void setup() {
    _helixManager = new MockManager(CLUSTER_NAME);
    _statsHolder = new StatsHolder(_helixManager, new HealthDataCache());
  }

  public Map<String, String> getStatFields(String value, String timestamp) {
    Map<String, String> statMap = new HashMap<String, String>();
    statMap.put(StatsHolder.VALUE_NAME, value);
    statMap.put(StatsHolder.TIMESTAMP_NAME, timestamp);
    return statMap;
  }

  public boolean statRecordContains(ZNRecord rec, String statName) {
    Map<String, Map<String, String>> stats = rec.getMapFields();
    return stats.containsKey(statName);
  }

  public boolean statRecordHasValue(ZNRecord rec, String statName, String value) {
    Map<String, Map<String, String>> stats = rec.getMapFields();
    Map<String, String> statFields = stats.get(statName);
    return (statFields.get(StatsHolder.VALUE_NAME).equals(value));
  }

  public boolean statRecordHasTimestamp(ZNRecord rec, String statName, String timestamp) {
    Map<String, Map<String, String>> stats = rec.getMapFields();
    Map<String, String> statFields = stats.get(statName);
    return (statFields.get(StatsHolder.TIMESTAMP_NAME).equals(timestamp));
  }

  // Exact matching persistent stat, but has no values yet
  @Test(groups = {
    "unitTest"
  })
  public void testAddFirstParticipantStat() throws Exception {
    // add a persistent stat
    String persistentStat = "accumulate()(dbFoo.partition10.latency)";
    _statsHolder.addStat(persistentStat);

    // generate incoming stat
    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("0", "0");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "0.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "0.0"));
  }

  // Exact matching persistent stat, but has no values yet
  @Test(groups = {
    "unitTest"
  })
  public void testAddRepeatParticipantStat() throws Exception {
    // add a persistent stat
    String persistentStat = "accumulate()(dbFoo.partition10.latency)";
    _statsHolder.addStat(persistentStat);

    // generate incoming stat
    String incomingStatName = "dbFoo.partition10.latency";
    // apply stat once and then again
    Map<String, String> statFields = getStatFields("0", "0");
    _statsHolder.applyStat(incomingStatName, statFields);
    statFields = getStatFields("1", "10");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "1.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "10.0"));
  }

  // test to ensure backdated stats not applied
  @Test(groups = {
    "unitTest"
  })
  public void testBackdatedParticipantStat() throws Exception {
    // add a persistent stat
    String persistentStat = "accumulate()(dbFoo.partition10.latency)";
    _statsHolder.addStat(persistentStat);

    // generate incoming stat
    String incomingStatName = "dbFoo.partition10.latency";
    // apply stat once and then again
    Map<String, String> statFields = getStatFields("0", "0");
    _statsHolder.applyStat(incomingStatName, statFields);
    statFields = getStatFields("1", "10");
    _statsHolder.applyStat(incomingStatName, statFields);
    statFields = getStatFields("5", "15");
    _statsHolder.applyStat(incomingStatName, statFields);
    statFields = getStatFields("1", "10");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "6.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "15.0"));
  }

  // Exact matching persistent stat, but has no values yet
  @Test(groups = {
    "unitTest"
  })
  public void testAddFirstParticipantStatToWildCard() throws Exception {
    // add a persistent stat
    String persistentWildcardStat = "accumulate()(dbFoo.partition*.latency)";
    _statsHolder.addStat(persistentWildcardStat);

    // generate incoming stat
    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("0", "0");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    String persistentStat = "accumulate()(dbFoo.partition10.latency)";
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "0.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "0.0"));
  }

  // test to add 2nd report to same stat
  @Test(groups = {
    "unitTest"
  })
  public void testAddSecondParticipantStatToWildCard() throws Exception {
    // add a persistent stat
    String persistentWildcardStat = "accumulate()(dbFoo.partition*.latency)";
    _statsHolder.addStat(persistentWildcardStat);

    // generate incoming stat
    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("1", "0");
    _statsHolder.applyStat(incomingStatName, statFields);
    statFields = getStatFields("1", "10");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    String persistentStat = "accumulate()(dbFoo.partition10.latency)";
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "2.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "10.0"));
  }

  // Exact matching persistent stat, but has no values yet
  @Test(groups = {
    "unitTest"
  })
  public void testAddParticipantStatToDoubleWildCard() throws Exception {
    // add a persistent stat
    String persistentWildcardStat = "accumulate()(db*.partition*.latency)";
    _statsHolder.addStat(persistentWildcardStat);

    // generate incoming stat
    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("0", "0");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    String persistentStat = "accumulate()(dbFoo.partition10.latency)";
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "0.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "0.0"));
  }

  @Test(groups = {
    "unitTest"
  })
  public void testAddWildcardInFirstStatToken() throws Exception {
    String persistentWildcardStat = "accumulate()(instance*.reportingage)";
    _statsHolder.addStat(persistentWildcardStat);

    // generate incoming stat
    String incomingStatName = "instance10.reportingage";
    Map<String, String> statFields = getStatFields("1", "10");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    String persistentStat = "accumulate()(instance10.reportingage)";
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "1.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "10.0"));

  }

  // test to add report to same wildcard stat, different actual stat
  @Test(groups = {
    "unitTest"
  })
  public void testAddTwoDistinctParticipantStatsToSameWildCard() throws Exception {
    // add a persistent stat
    String persistentWildcardStat = "accumulate()(dbFoo.partition*.latency)";
    _statsHolder.addStat(persistentWildcardStat);

    // generate incoming stat
    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("1", "10");
    _statsHolder.applyStat(incomingStatName, statFields);
    incomingStatName = "dbFoo.partition11.latency";
    statFields = getStatFields("5", "10");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    String persistentStat = "accumulate()(dbFoo.partition10.latency)";
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "1.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "10.0"));
    persistentStat = "accumulate()(dbFoo.partition11.latency)";
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "5.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "10.0"));
  }

  // Exact matching persistent stat, but has no values yet
  @Test(groups = {
    "unitTest"
  })
  public void testWindowStat() throws Exception {
    // add a persistent stat
    String persistentWildcardStat = "window(3)(dbFoo.partition*.latency)";
    _statsHolder.addStat(persistentWildcardStat);

    // generate incoming stat
    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("0", "0");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    String persistentStat = "window(3)(dbFoo.partition10.latency)";
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "0.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "0.0"));

    // add 2nd stat
    statFields = getStatFields("10", "1");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "0.0,10.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "0.0,1.0"));

    // add 3rd stat
    statFields = getStatFields("20", "2");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "0.0,10.0,20.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "0.0,1.0,2.0"));

  }

  @Test(groups = {
    "unitTest"
  })
  public void testWindowStatExpiration() throws Exception {
    String persistentWildcardStat = "window(3)(dbFoo.partition*.latency)";
    String persistentStat = "window(3)(dbFoo.partition10.latency)";
    // init with 3 elements
    testWindowStat();

    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("30", "3");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "10.0,20.0,30.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "1.0,2.0,3.0"));
  }

  @Test(groups = {
    "unitTest"
  })
  public void testWindowStatStale() throws Exception {
    String persistentWildcardStat = "window(3)(dbFoo.partition*.latency)";
    String persistentStat = "window(3)(dbFoo.partition10.latency)";
    // init with 3 elements
    testWindowStat();

    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("10", "1");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "0.0,10.0,20.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "0.0,1.0,2.0"));
  }

  // test that has 2 agg stats for same raw stat
  // Exact matching persistent stat, but has no values yet
  @Test(groups = {
    "unitTest"
  })
  public void testAddStatForTwoAggTypes() throws Exception {
    // add a persistent stat
    String persistentStatOne = "accumulate()(dbFoo.partition10.latency)";
    String persistentStatTwo = "window(3)(dbFoo.partition10.latency)";
    _statsHolder.addStat(persistentStatOne);
    _statsHolder.persistStats();
    _statsHolder.addStat(persistentStatTwo);
    _statsHolder.persistStats();

    // generate incoming stat
    String incomingStatName = "dbFoo.partition10.latency";
    Map<String, String> statFields = getStatFields("0", "0");
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();

    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStatOne, "0.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStatOne, "0.0"));
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStatTwo, "0.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStatTwo, "0.0"));
  }

  // test merging 2 window stats, new is applied
  @Test(groups = {
    "unitTest"
  })
  public void testMergeTwoWindowsYesMerge() throws Exception {
    String persistentWildcardStat = "window(3)(dbFoo.partition*.latency)";
    String persistentStat = "window(3)(dbFoo.partition10.latency)";
    String incomingStatName = "dbFoo.partition10.latency";
    // init with 3 elements
    testWindowStat();

    // create a two tuples, value and time
    Tuple<String> valTuple = new Tuple<String>();
    Tuple<String> timeTuple = new Tuple<String>();
    valTuple.add("30.0");
    valTuple.add("40.0");
    timeTuple.add("3.0");
    timeTuple.add("4.0");
    Map<String, String> statFields = getStatFields(valTuple.toString(), timeTuple.toString());
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();
    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "20.0,30.0,40.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "2.0,3.0,4.0"));
  }

  // test merging 2 window stats, new is ignored
  @Test(groups = {
    "unitTest"
  })
  public void testMergeTwoWindowsNoMerge() throws Exception {
    String persistentWildcardStat = "window(3)(dbFoo.partition*.latency)";
    String persistentStat = "window(3)(dbFoo.partition10.latency)";
    String incomingStatName = "dbFoo.partition10.latency";
    // init with 3 elements
    testWindowStat();

    // create a two tuples, value and time
    Tuple<String> valTuple = new Tuple<String>();
    Tuple<String> timeTuple = new Tuple<String>();
    valTuple.add("0.0");
    valTuple.add("40.0");
    timeTuple.add("0.0");
    timeTuple.add("4.0");
    Map<String, String> statFields = getStatFields(valTuple.toString(), timeTuple.toString());
    _statsHolder.applyStat(incomingStatName, statFields);
    _statsHolder.persistStats();

    // check persistent stats
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();

    ZNRecord rec = accessor.getProperty(keyBuilder.persistantStat()).getRecord();
    System.out.println("rec: " + rec.toString());
    AssertJUnit.assertTrue(statRecordHasValue(rec, persistentStat, "0.0,10.0,20.0"));
    AssertJUnit.assertTrue(statRecordHasTimestamp(rec, persistentStat, "0.0,1.0,2.0"));
  }
}
