package org.apache.helix.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The history of participant.
 */
public class ParticipantHistory extends HelixProperty {
  private static Logger LOG = LoggerFactory.getLogger(ParticipantHistory.class);
  private static final String UNKNOWN_HOST_NAME = "UnknownHostname";

  private final static int HISTORY_SIZE = 20;
  private final static String HISTORY_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss:SSS";

  enum ConfigProperty {
    TIME,
    DATE,
    SESSION,
    HISTORY,
    OFFLINE,
    VERSION,
    LAST_OFFLINE_TIME,
    HOST
  }

  public static long ONLINE = -1;

  public ParticipantHistory(String id) {
    super(id);
  }

  public ParticipantHistory(ZNRecord znRecord) {
    super(znRecord);
  }

  /**
   * Called when a participant went offline or is about to go offline.
   * This will update the offline timestamp in participant history.
   */
  public void reportOffline() {
    long time = System.currentTimeMillis();
    _record.setSimpleField(ConfigProperty.LAST_OFFLINE_TIME.name(), String.valueOf(time));
    updateOfflineHistory(time);
  }

  /**
   * Called when a participant goes online, this will update all related session history.
   *
   * @return
   */
  public void reportOnline(String sessionId, String version) {
    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Failed to get host name. Use {} for the participant history recording.",
          UNKNOWN_HOST_NAME);
      hostname = UNKNOWN_HOST_NAME;
    }
    updateSessionHistory(sessionId, version, hostname);
    _record.setSimpleField(ConfigProperty.LAST_OFFLINE_TIME.name(), String.valueOf(ONLINE));
  }

  /**
   * Get the time when this node goes offline last time (epoch time). If the node is currently
   * online or if no offline time is recorded, return -1.
   *
   * @return
   */
  public long getLastOfflineTime() {
    long offlineTime = ONLINE;
    String timeStr = _record.getSimpleField(ConfigProperty.LAST_OFFLINE_TIME.name());
    if (timeStr != null) {
      try {
        offlineTime = Long.valueOf(timeStr);
      } catch (NumberFormatException ex) {
        LOG.warn("Failed to parse LAST_OFFLINE_TIME " + timeStr);
      }
    }

    return offlineTime;
  }

  /**
   * Get the time when this node last goes offline in history. If the node does not have offline
   * history or contains invalid date as the last element, return -1.
   *
   * @return
   */
  public long getLastTimeInOfflineHistory() {
    List<String> offlineHistory = _record.getListField(ConfigProperty.OFFLINE.name());
    if (offlineHistory == null || offlineHistory.isEmpty()) {
      return -1;
    }

    String lastDate = offlineHistory.get(offlineHistory.size() - 1);
    return historyDateStringToLong(lastDate);
  }

  /**
   * Add record to session online history list
   */
  private void updateSessionHistory(String sessionId, String version, String hostname) {
    List<String> list = _record.getListField(ConfigProperty.HISTORY.name());
    if (list == null) {
      list = new ArrayList<>();
      _record.setListField(ConfigProperty.HISTORY.name(), list);
    }

    if (list.size() == HISTORY_SIZE) {
      list.remove(0);
    }

    Map<String, String> sessionEntry = new HashMap<String, String>();

    sessionEntry.put(ConfigProperty.SESSION.name(), sessionId);

    long timeMillis = System.currentTimeMillis();
    sessionEntry.put(ConfigProperty.TIME.name(), String.valueOf(timeMillis));

    sessionEntry.put(ConfigProperty.DATE.name(), historyDateLongToString(timeMillis));
    sessionEntry.put(ConfigProperty.VERSION.name(), version);
    sessionEntry.put(ConfigProperty.HOST.name(), hostname);

    list.add(sessionEntry.toString());
  }

  private void updateOfflineHistory(long time) {
    List<String> list = _record.getListField(ConfigProperty.OFFLINE.name());
    if (list == null) {
      list = new ArrayList<>();
      _record.setListField(ConfigProperty.OFFLINE.name(), list);
    }

    if (list.size() == HISTORY_SIZE) {
      list.remove(0);
    }
    list.add(historyDateLongToString(time));
  }

  @Override
  public boolean isValid() {
    return true;
  }

  /*
   * Parses a history date in string format to its millisecond representation.
   * Returns -1 if parsing fails.
   */
  private static long historyDateStringToLong(String dateString) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(HISTORY_DATE_FORMAT);
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    try {
      Date date = simpleDateFormat.parse(dateString);
      return date.getTime();
    } catch (ParseException e) {
      LOG.warn("Failed to parse participant history date string: " + dateString);
      return -1;
    }
  }

  /*
   * Parses a history date in millisecond to string.
   */
  private static String historyDateLongToString(long dateLong) {
    DateFormat df = new SimpleDateFormat(HISTORY_DATE_FORMAT);
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    return df.format(new Date(dateLong));
  }

  /**
   * Parses the session entry map that has been converted to string back to a map.
   * NOTE TO CALLER: This assumes the divider between entries is ", " and the divider between
   * key/value is "="; if the string is malformed, parsing correctness is not guaranteed. Always
   * check if a key is contained before using the key.
   */
  public static Map<String, String> sessionHistoryStringToMap(String sessionHistoryString) {
    sessionHistoryString = sessionHistoryString.substring(1, sessionHistoryString.length() - 1);
    Map<String, String> sessionHistoryMap = new HashMap<>();

    for (String sessionHistoryKeyValuePair : sessionHistoryString.split(", ")) {
      String[] keyValuePair = sessionHistoryKeyValuePair.split("=");
      if (keyValuePair.length < 2) {
        LOG.warn("Ignore key value pair while parsing session history due to missing '=': " +
            sessionHistoryKeyValuePair);
        continue;
      }
      sessionHistoryMap.put(keyValuePair[0], keyValuePair[1]);
    }

    return sessionHistoryMap;
  }

  /*
   * Take a string session history entry and extract the TIME field out of it. Return -1 if the TIME
   * field doesn't exist or if the TIME field cannot be parsed to a long.
   */
  private static long getTimeFromSessionHistoryString(String sessionHistoryString) {
    Map<String, String> sessionHistoryMap = sessionHistoryStringToMap(sessionHistoryString);
    if (!sessionHistoryMap.containsKey(ConfigProperty.TIME.name())) {
      return -1;
    }
    try {
      return Long.parseLong(sessionHistoryMap.get(ConfigProperty.TIME.name()));
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse TIME field to long: " + sessionHistoryMap
          .get(ConfigProperty.TIME.name()));
      return -1;
    }
  }

  /**
   * For each entry in History, return its millisecond timestamp; for timestamps that cannot be
   * parsed, skip them.
   */
  public List<Long> getOnlineTimestampsAsMilliseconds() {
    List<String> historyList = getRecord().getListField(ConfigProperty.HISTORY.name());
    if (historyList == null) {
      return Collections.emptyList();
    }
    return historyList.stream().map(ParticipantHistory::getTimeFromSessionHistoryString)
        .filter(result -> result != -1).collect(Collectors.toList());
  }

  /**
   * For each entry in Offline, return it as a millisecond timestamp; for timestamps that cannot be
   * parsed, skip them.
   */
  public List<Long> getOfflineTimestampsAsMilliseconds() {
    List<String> offlineList = getRecord().getListField(ConfigProperty.OFFLINE.name());
    if (offlineList == null) {
      return Collections.emptyList();
    }
    return offlineList.stream().map(ParticipantHistory::historyDateStringToLong)
        .filter(result -> result != -1).collect(Collectors.toList());
  }
}
