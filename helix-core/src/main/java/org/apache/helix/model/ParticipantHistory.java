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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.log4j.Logger;

/**
 * The history of participant.
 */
public class ParticipantHistory extends HelixProperty {
  private static Logger LOG = Logger.getLogger(ParticipantHistory.class);

  private final static int HISTORY_SIZE = 20;
  private enum ConfigProperty {
    TIME,
    DATE,
    SESSION,
    HISTORY,
    OFFLINE,
    LAST_OFFLINE_TIME
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
  public void reportOnline(String sessionId) {
    updateSessionHistory(sessionId);
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
   * Add record to session online history list
   */
  private void updateSessionHistory(String sessionId) {
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

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateTime = df.format(new Date(timeMillis));
    sessionEntry.put(ConfigProperty.DATE.name(), dateTime);

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

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateTime = df.format(new Date(time));

    list.add(dateTime);
  }

  @Override
  public boolean isValid() {
    return true;
  }
}
