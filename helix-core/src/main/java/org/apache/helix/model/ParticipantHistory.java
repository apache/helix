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

/**
 * The history of participant.
 */
public class ParticipantHistory extends HelixProperty {
  private final static int HISTORY_SIZE = 10;
  private enum ConfigProperty {
    TIME,
    DATE,
    SESSION,
    HISTORY
  }

  public ParticipantHistory(String id) {
    super(id);
  }

  public ParticipantHistory(ZNRecord znRecord) {
    super(znRecord);
  }

  /**
   * Update last offline timestamp in participant history.
   *
   * @return
   */
  public void updateHistory(String sessionId) {
    List<String> list = _record.getListField(ConfigProperty.HISTORY.name());
    if (list == null) {
      list = new ArrayList<String>();
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

  @Override
  public boolean isValid() {
    return true;
  }
}
