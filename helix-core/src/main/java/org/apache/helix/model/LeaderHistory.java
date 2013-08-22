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

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

/**
 * The history of instances that have served as the leader controller
 */
public class LeaderHistory extends HelixProperty {
  private final static int HISTORY_SIZE = 8;

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
  public void updateHistory(String clusterName, String instanceName) {
    List<String> list = _record.getListField(clusterName);
    if (list == null) {
      list = new ArrayList<String>();
      _record.setListField(clusterName, list);
    }

    if (list.size() == HISTORY_SIZE) {
      list.remove(0);
    }
    list.add(instanceName);
  }

  @Override
  public boolean isValid() {
    return true;
  }
}
