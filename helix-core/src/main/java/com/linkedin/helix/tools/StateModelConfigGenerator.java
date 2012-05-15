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
package com.linkedin.helix.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.model.StateModelDefinition.StateModelDefinitionProperty;

public class StateModelConfigGenerator
{

  public static void main(String[] args)
  {
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    System.out.println(new String(serializer.serialize(generator.generateConfigForMasterSlave())));
  }

  /**
   * count -1 dont care any numeric value > 0 will be tried to be satisfied based on
   * priority N all nodes in the cluster will be assigned to this state if possible R all
   * remaining nodes in the preference list will be assigned to this state, applies only
   * to last state
   */

  public ZNRecord generateConfigForStorageSchemata()
  {
    ZNRecord record = new ZNRecord("STORAGE_DEFAULT_SM_SCHEMATA");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(),
                          "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("MASTER");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    statePriorityList.add("ERROR");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
                        statePriorityList);
    for (String state : statePriorityList)
    {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("MASTER"))
      {
        metadata.put("count", "N");
        record.setMapField(key, metadata);
      }
      else if (state.equals("OFFLINE"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      else if (state.equals("DROPPED"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      else if (state.equals("ERROR"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }
    for (String state : statePriorityList)
    {
      String key = state + ".next";
      if (state.equals("MASTER"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("MASTER", "MASTER");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      }
      if (state.equals("ERROR"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("MASTER-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-MASTER");
    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
                        stateTransitionPriorityList);
    return record;
  }

  public ZNRecord generateConfigForMasterSlave()
  {
    ZNRecord record = new ZNRecord("MasterSlave");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(),
                          "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("MASTER");
    statePriorityList.add("SLAVE");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    statePriorityList.add("ERROR");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
                        statePriorityList);
    for (String state : statePriorityList)
    {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("MASTER"))
      {
        metadata.put("count", "1");
        record.setMapField(key, metadata);
      }
      else if (state.equals("SLAVE"))
      {
        metadata.put("count", "R");
        record.setMapField(key, metadata);
      }
      else if (state.equals("OFFLINE"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      else if (state.equals("DROPPED"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      else if (state.equals("ERROR"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }
    for (String state : statePriorityList)
    {
      String key = state + ".next";
      if (state.equals("MASTER"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("SLAVE", "SLAVE");
        metadata.put("OFFLINE", "SLAVE");
        metadata.put("DROPPED", "SLAVE");
        record.setMapField(key, metadata);
      }
      else if (state.equals("SLAVE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("MASTER", "MASTER");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      }
      else if (state.equals("OFFLINE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("SLAVE", "SLAVE");
        metadata.put("MASTER", "SLAVE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      }
      else if (state.equals("ERROR"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("MASTER-SLAVE");
    stateTransitionPriorityList.add("SLAVE-MASTER");
    stateTransitionPriorityList.add("OFFLINE-SLAVE");
    stateTransitionPriorityList.add("SLAVE-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");
    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
                        stateTransitionPriorityList);
    return record;
    // ZNRecordSerializer serializer = new ZNRecordSerializer();
    // System.out.println(new String(serializer.serialize(record)));
  }

  public ZNRecord generateConfigForLeaderStandby()
  {
    ZNRecord record = new ZNRecord("LeaderStandby");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(),
                          "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("LEADER");
    statePriorityList.add("STANDBY");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
                        statePriorityList);
    for (String state : statePriorityList)
    {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("LEADER"))
      {
        metadata.put("count", "1");
        record.setMapField(key, metadata);
      }
      if (state.equals("STANDBY"))
      {
        metadata.put("count", "R");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      if (state.equals("DROPPED"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }

    }

    for (String state : statePriorityList)
    {
      String key = state + ".next";
      if (state.equals("LEADER"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("STANDBY", "STANDBY");
        metadata.put("OFFLINE", "STANDBY");
        metadata.put("DROPPED", "STANDBY");
        record.setMapField(key, metadata);
      }
      if (state.equals("STANDBY"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("LEADER", "LEADER");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("STANDBY", "STANDBY");
        metadata.put("LEADER", "STANDBY");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      }

    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("LEADER-STANDBY");
    stateTransitionPriorityList.add("STANDBY-LEADER");
    stateTransitionPriorityList.add("OFFLINE-STANDBY");
    stateTransitionPriorityList.add("STANDBY-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");

    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
                        stateTransitionPriorityList);
    return record;
    // ZNRecordSerializer serializer = new ZNRecordSerializer();
    // System.out.println(new String(serializer.serialize(record)));
  }

  public ZNRecord generateConfigForOnlineOffline()
  {
    ZNRecord record = new ZNRecord("OnlineOffline");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(),
                          "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("ONLINE");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
                        statePriorityList);
    for (String state : statePriorityList)
    {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("ONLINE"))
      {
        metadata.put("count", "N");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      if (state.equals("DROPPED"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }

    for (String state : statePriorityList)
    {
      String key = state + ".next";
      if (state.equals("ONLINE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("ONLINE", "ONLINE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("OFFLINE-ONLINE");
    stateTransitionPriorityList.add("ONLINE-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");

    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
                        stateTransitionPriorityList);
    return record;
    // ZNRecordSerializer serializer = new ZNRecordSerializer();
    // System.out.println(new String(serializer.serialize(record)));
  }
}
