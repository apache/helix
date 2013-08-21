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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.stages.HealthDataCache;
import org.apache.helix.model.PersistentStats;
import org.apache.log4j.Logger;

public class StatsHolder {
  enum MatchResult {
    WILDCARDMATCH,
    EXACTMATCH,
    NOMATCH
  };

  private static final Logger logger = Logger.getLogger(StatsHolder.class.getName());

  public static final String VALUE_NAME = "value";
  public static final String TIMESTAMP_NAME = "TimeStamp";

  HelixDataAccessor _accessor;
  HealthDataCache _cache;

  Map<String, Map<String, String>> _statMap;
  Map<String, Map<String, MatchResult>> _statAlertMatchResult;

  private Builder _keyBuilder;

  // PersistentStats _persistentStats;

  public StatsHolder(HelixManager manager, HealthDataCache cache) {
    _accessor = manager.getHelixDataAccessor();
    _cache = cache;
    _keyBuilder = new PropertyKey.Builder(manager.getClusterName());
    updateCache(_cache);
    _statAlertMatchResult = new HashMap<String, Map<String, MatchResult>>();

  }

  public void refreshStats() {
    logger.info("Refreshing cached stats");
    _cache.refresh(_accessor);
    updateCache(_cache);
  }

  public void persistStats() {
    // XXX: Am I using _accessor too directly here?
    // took around 35 ms from desktop to ESV4 machine
    PersistentStats stats = _accessor.getProperty(_keyBuilder.persistantStat());
    if (stats == null) {
      stats = new PersistentStats(PersistentStats.nodeName); // TODO: fix naming of
      // this record, if it
      // matters
    }
    stats.getRecord().setMapFields(_statMap);
    boolean retVal = _accessor.setProperty(_keyBuilder.persistantStat(), stats);
  }

  public void getStatsFromCache(boolean refresh) {
    long refreshStartTime = System.currentTimeMillis();
    if (refresh) {
      _cache.refresh(_accessor);
    }
    PersistentStats persistentStatRecord = _cache.getPersistentStats();
    if (persistentStatRecord != null) {
      _statMap = persistentStatRecord.getMapFields();
    } else {
      _statMap = new HashMap<String, Map<String, String>>();
    }
    /*
     * if (_cache.getPersistentStats() != null) {
     * _statMap = _cache.getPersistentStats();
     * }
     */
    // TODO: confirm this a good place to init the _statMap when null
    /*
     * if (_statMap == null) {
     * _statMap = new HashMap<String, Map<String, String>>();
     * }
     */
    System.out.println("Refresh stats done: " + (System.currentTimeMillis() - refreshStartTime));
  }

  public Iterator<String> getAllStats() {
    return null;
  }

  /*
   * TODO: figure out pre-conditions here. I think not allowing anything to be
   * null on input
   */
  public Map<String, String> mergeStats(String statName, Map<String, String> existingStat,
      Map<String, String> incomingStat) throws HelixException {
    if (existingStat == null) {
      throw new HelixException("existing stat for merge is null");
    }
    if (incomingStat == null) {
      throw new HelixException("incoming stat for merge is null");
    }
    // get agg type and arguments, then get agg object
    String aggTypeStr = ExpressionParser.getAggregatorStr(statName);
    String[] aggArgs = ExpressionParser.getAggregatorArgs(statName);
    Aggregator agg = ExpressionParser.getAggregator(aggTypeStr);
    // XXX: some of below lines might fail with null exceptions

    // get timestamps, values out of zk maps
    String existingTime = existingStat.get(TIMESTAMP_NAME);
    String existingVal = existingStat.get(VALUE_NAME);
    String incomingTime = incomingStat.get(TIMESTAMP_NAME);
    String incomingVal = incomingStat.get(VALUE_NAME);
    // parse values into tuples, if the values exist. else, tuples are null
    Tuple<String> existingTimeTuple =
        (existingTime != null) ? Tuple.fromString(existingTime) : null;
    Tuple<String> existingValueTuple = (existingVal != null) ? Tuple.fromString(existingVal) : null;
    Tuple<String> incomingTimeTuple =
        (incomingTime != null) ? Tuple.fromString(incomingTime) : null;
    Tuple<String> incomingValueTuple = (incomingVal != null) ? Tuple.fromString(incomingVal) : null;

    // dp merge
    agg.merge(existingValueTuple, incomingValueTuple, existingTimeTuple, incomingTimeTuple, aggArgs);
    // put merged tuples back in map
    Map<String, String> mergedMap = new HashMap<String, String>();
    if (existingTimeTuple.size() == 0) {
      throw new HelixException("merged time tuple has size zero");
    }
    if (existingValueTuple.size() == 0) {
      throw new HelixException("merged value tuple has size zero");
    }

    mergedMap.put(TIMESTAMP_NAME, existingTimeTuple.toString());
    mergedMap.put(VALUE_NAME, existingValueTuple.toString());
    return mergedMap;
  }

  /*
   * Find all persisted stats this stat matches. Update those stats. An incoming
   * stat can match multiple stats exactly (if that stat has multiple agg types)
   * An incoming stat can match multiple wildcard stats
   */

  // need to do a time check here!

  public void applyStat(String incomingStatName, Map<String, String> statFields) {
    // TODO: consider locking stats here
    // refreshStats(); //will have refreshed by now during stage

    Map<String, Map<String, String>> pendingAdds = new HashMap<String, Map<String, String>>();

    if (!_statAlertMatchResult.containsKey(incomingStatName)) {
      _statAlertMatchResult.put(incomingStatName, new HashMap<String, MatchResult>());
    }
    Map<String, MatchResult> resultMap = _statAlertMatchResult.get(incomingStatName);
    // traverse through all persistent stats
    for (String key : _statMap.keySet()) {
      if (resultMap.containsKey(key)) {
        MatchResult cachedMatchResult = resultMap.get(key);
        if (cachedMatchResult == MatchResult.EXACTMATCH) {
          processExactMatch(key, statFields);
        } else if (cachedMatchResult == MatchResult.WILDCARDMATCH) {
          processWildcardMatch(incomingStatName, key, statFields, pendingAdds);
        }
        // don't care about NOMATCH
        continue;
      }
      // exact match on stat and stat portion of persisted stat, just update
      if (ExpressionParser.isIncomingStatExactMatch(key, incomingStatName)) {
        processExactMatch(key, statFields);
        resultMap.put(key, MatchResult.EXACTMATCH);
      }
      // wildcard match
      else if (ExpressionParser.isIncomingStatWildcardMatch(key, incomingStatName)) {
        processWildcardMatch(incomingStatName, key, statFields, pendingAdds);
        resultMap.put(key, MatchResult.WILDCARDMATCH);
      } else {
        resultMap.put(key, MatchResult.NOMATCH);
      }
    }
    _statMap.putAll(pendingAdds);
  }

  void processExactMatch(String key, Map<String, String> statFields) {
    Map<String, String> mergedStat = mergeStats(key, _statMap.get(key), statFields);
    // update in place, no problem with hash map
    _statMap.put(key, mergedStat);
  }

  void processWildcardMatch(String incomingStatName, String key, Map<String, String> statFields,
      Map<String, Map<String, String>> pendingAdds) {

    // make sure incoming stat doesn't already exist, either in previous
    // round or this round
    // form new key (incomingStatName with agg type from the wildcarded
    // stat)
    String statToAdd = ExpressionParser.getWildcardStatSubstitution(key, incomingStatName);
    // if the stat already existed in _statMap, we have/will apply it as an
    // exact match
    // if the stat was added this round to pendingAdds, no need to recreate
    // (it would have same value)
    if (!_statMap.containsKey(statToAdd) && !pendingAdds.containsKey(statToAdd)) {
      // add this stat to persisted stats
      Map<String, String> mergedStat = mergeStats(statToAdd, getEmptyStat(), statFields);
      // add to pendingAdds so we don't mess up ongoing traversal of
      // _statMap
      pendingAdds.put(statToAdd, mergedStat);
    }
  }

  // add parsing of stat (or is that in expression holder?) at least add
  // validate
  public void addStat(String exp) throws HelixException {
    refreshStats(); // get current stats

    String[] parsedStats = ExpressionParser.getBaseStats(exp);

    for (String stat : parsedStats) {
      if (_statMap.containsKey(stat)) {
        logger.debug("Stat " + stat + " already exists; not adding");
        continue;
      }
      _statMap.put(stat, getEmptyStat()); // add new stat to map
    }
  }

  public static Map<String, Map<String, String>> parseStat(String exp) throws HelixException {
    String[] parsedStats = ExpressionParser.getBaseStats(exp);
    Map<String, Map<String, String>> statMap = new HashMap<String, Map<String, String>>();

    for (String stat : parsedStats) {
      if (statMap.containsKey(stat)) {
        logger.debug("Stat " + stat + " already exists; not adding");
        continue;
      }
      statMap.put(stat, getEmptyStat()); // add new stat to map
    }
    return statMap;
  }

  public static Map<String, String> getEmptyStat() {
    Map<String, String> statFields = new HashMap<String, String>();
    statFields.put(TIMESTAMP_NAME, "");
    statFields.put(VALUE_NAME, "");
    return statFields;
  }

  public List<Stat> getStatsList() {
    List<Stat> stats = new LinkedList<Stat>();
    for (String stat : _statMap.keySet()) {
      Map<String, String> statFields = _statMap.get(stat);
      Tuple<String> valTup = Tuple.fromString(statFields.get(VALUE_NAME));
      Tuple<String> timeTup = Tuple.fromString(statFields.get(TIMESTAMP_NAME));
      Stat s = new Stat(stat, valTup, timeTup);
      stats.add(s);
    }
    return stats;
  }

  public Map<String, Tuple<String>> getStatsMap() {
    // refreshStats(); //don't refresh, stage will have refreshed by this time
    HashMap<String, Tuple<String>> stats = new HashMap<String, Tuple<String>>();
    for (String stat : _statMap.keySet()) {
      Map<String, String> statFields = _statMap.get(stat);
      Tuple<String> valTup = Tuple.fromString(statFields.get(VALUE_NAME));
      Tuple<String> timeTup = Tuple.fromString(statFields.get(TIMESTAMP_NAME));
      stats.put(stat, valTup);
    }
    return stats;
  }

  public void updateCache(HealthDataCache cache) {
    _cache = cache;
    PersistentStats persistentStatRecord = _cache.getPersistentStats();
    if (persistentStatRecord != null) {
      _statMap = persistentStatRecord.getMapFields();
    } else {
      _statMap = new HashMap<String, Map<String, String>>();
    }
  }
}
