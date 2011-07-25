package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;

public class StateModelConfigGenerator
{

  public static void main(String[] args)
  {
    generateConfigForStorage();
  }

  private static void generateConfigForStorage()
  {
    ZNRecord record = new ZNRecord();
    record.setId("STORAGE_DEFAULT_SM_DEF");
    record.setSimpleField("INITIAL_STATE", "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("MASTER");
    statePriorityList.add("SLAVE");
    statePriorityList.add("OFFLINE");
    record.setListField("statesPriorityList", statePriorityList);
    for (String state : statePriorityList)
    {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("MASTER")){
        metadata.put("max", "1");
        metadata.put("min", "1");
        record.setMapField(key, metadata);
      }
      if (state.equals("SLAVE")){
        metadata.put("max", "3");
        metadata.put("min", "0");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE")){
        metadata.put("max", "-1");
        metadata.put("min", "-1");
        record.setMapField(key, metadata);
      }
    }
    for (String state : statePriorityList)
    {
      String key = state + ".next";
      
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("SLAVE", "SLAVE");
        metadata.put("OFFLINE", "SLAVE");
        record.setMapField(key, metadata);
      }
      if (state.equals("SLAVE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("MASTER", "MASTER");
        metadata.put("OFFLINE", "OFFLINE");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("SLAVE", "SLAVE");
        metadata.put("MASTER", "SLAVE");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("MASTER-SLAVE");
    stateTransitionPriorityList.add("SLAVE-MASTER");
    stateTransitionPriorityList.add("OFFLINE-SLAVE");
    stateTransitionPriorityList.add("SLAVE-OFFLINE");
    record.setListField("stateTransitionPriorityList",
        stateTransitionPriorityList);
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    System.out.println(new String(serializer.serialize(record)));
  }
}
