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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

/**
 * The history of instances that have served as the leader controller
 */
public class LeaderHistory extends HelixProperty {
  private final static int HISTORY_SIZE = 10;

  private enum ConfigProperty {
    HISTORY,
    TIME,
    DATE,
    VERSION,
    CONTROLLER
  }

  public LeaderHistory(String id) {
    super(id);
  }

  public LeaderHistory(ZNRecord record) {
    super(record);
  }

  /**
   * Save up to HISTORY_SIZE number of leaders in FIFO order
   * @param clusterName the cluster the instance leads
   * @param instanceName the name of the leader instance
   */
  public void updateHistory(String clusterName, String instanceName, String version) {
    /* keep this for back-compatible */
    // TODO: remove this in future when we confirmed no one consumes it
    List<String> list = _record.getListField(clusterName);
    if (list == null) {
      list = new ArrayList<String>();
      _record.setListField(clusterName, list);
    }

    if (list.size() == HISTORY_SIZE) {
      list.remove(0);
    }
    list.add(instanceName);
    // TODO: remove above in future when we confirmed no one consumes it */

    List<String> historyList = _record.getListField(ConfigProperty.HISTORY.name());
    if (historyList == null) {
      historyList = new ArrayList<String>();
      _record.setListField(ConfigProperty.HISTORY.name(), historyList);
    }

    if (historyList.size() == HISTORY_SIZE) {
      historyList.remove(0);
    }

    Map<String, String> historyEntry = new HashMap<String, String>();

    long currentTime = System.currentTimeMillis();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateTime = df.format(new Date(currentTime));

    historyEntry.put(ConfigProperty.CONTROLLER.name(), instanceName);
    historyEntry.put(ConfigProperty.TIME.name(), String.valueOf(currentTime));
    historyEntry.put(ConfigProperty.DATE.name(), dateTime);
    historyEntry.put(ConfigProperty.VERSION.name(), version);

    historyList.add(historyEntry.toString());
  }

  /**
   * Get history list
   * @return
   */
  public List<String> getHistoryList() {
    List<String> historyList = _record.getListField(ConfigProperty.HISTORY.name());
    if (historyList == null) {
      historyList = new ArrayList<String>();
    }

    return historyList;
  }

  @Override
  public boolean isValid() {
    return true;
  }
}
