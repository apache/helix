package org.apache.helix.taskexecution;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;

public class JoinTask extends Task {
  public static final String JOINED_CLICKS = "joined_clicks_demo";

  private final String _impressionList;
  private final String _clickList;

  public JoinTask(String id, Set<String> parentIds, HelixManager helixManager,
      TaskResultStore resultStore, String impressionList, String clickList) {
    super(id, parentIds, helixManager, resultStore);
    _impressionList = impressionList;
    _clickList = clickList;
  }

  @Override
  protected void executeImpl(String resourceName, int numPartitions, int partitionNum)
      throws Exception {
    System.out.println("Executing JoinTask for " + resourceName + "_" + partitionNum);
    long numClicks = resultStore.llen(_clickList);
    List<String> clickEvents = resultStore.lrange(_clickList, 0, numClicks - 1);
    Map<String, String[]> clickIndex = getClickIndex(clickEvents);

    long len = resultStore.llen(_impressionList);
    long bucketSize = len / numPartitions;
    long start = partitionNum * bucketSize;
    long end = start + bucketSize - 1;
    List<String> impressions = resultStore.lrange(_impressionList, start, end);
    for (String impression : impressions) {
      String[] fields = impression.split(",");
      if (clickIndex.containsKey(fields[0])) {
        String clickId = clickIndex.get(fields[0])[0];
        String joinedClick = clickId + "," + impression;
        resultStore.rpush(JOINED_CLICKS, joinedClick);
      }
    }
  }

  // return map of impression id to click (fields of the click event)
  private Map<String, String[]> getClickIndex(List<String> clickEvents) {
    Map<String, String[]> clickIndex = new HashMap<String, String[]>();
    for (String click : clickEvents) {
      String fields[] = click.split(",");
      clickIndex.put(fields[2], fields);
    }
    return clickIndex;
  }

}
