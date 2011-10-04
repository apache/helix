package com.linkedin.clustermanager.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.Criteria;

public class CriteriaEvaluator
{

  public List<Map<String, String>> evaluateCriteria(
      List<Map<String, String>> rows, Criteria recipientCriteria)
  {
    List<Map<String, String>> selected = new ArrayList<Map<String, String>>();
    HashSet<String> selectedSet = new HashSet<String>();
    String instanceName = recipientCriteria.getInstanceName();
    String resourceGroup = recipientCriteria.getResourceGroup();
    String resourceKey = recipientCriteria.getResourceKey();
    String state = recipientCriteria.getResourceState();

    boolean anyInstance = (instanceName == null || instanceName.equals("*"));
    boolean anyResourceGroup = (resourceGroup == null || resourceGroup
        .equals("*"));
    boolean anyResourceKey = (resourceKey == null || resourceKey.equals("*"));
    boolean anyState = (state == null || state.equals("*"));
    for (Map<String, String> row : rows)
    {
      HashMap<String, String> resultRow = new HashMap<String, String>();
      if (!anyInstance
          && !instanceName.equalsIgnoreCase(row.get("instanceName")))
      {
        continue;
      }
      else
      {
        if(instanceName != null)
        {
          resultRow.put("instanceName", row.get("instanceName"));
        }
      }
      
      if (!anyResourceGroup
          && !resourceGroup.equalsIgnoreCase(row.get("resourceGroup")))
      {
        continue;
      }
      else
      {
        if(resourceGroup!=null)
        {
          resultRow.put("resourceGroup", row.get("resourceGroup"));
        }
      }
      
      if (!anyResourceKey
          && !resourceKey.equalsIgnoreCase(row.get("resourceKey")))
      {
        continue;
      }
      else
      {
        if(resourceKey != null)
        {
          resultRow.put("resourceKey", row.get("resourceKey"));
        }
      }
      
      if (!anyState && !state.equalsIgnoreCase(row.get("state")))
      {
        continue;
      }
      else
      {
        if(state!=null)
        {
          resultRow.put("state", row.get("state"));
        }
      }
      String mapString = getMapString(resultRow);
      if(!selectedSet.contains(mapString))
      {
        selectedSet.add(mapString);
        selected.add(resultRow);
      }
    }
    return selected;
  }
  
  private String getMapString(Map<String, String> map)
  {
    String result = "";
    
    String[] keys = new String[map.size()];
    map.keySet().toArray(keys);
    Arrays.sort(keys);
    for(String key : keys)
    {
      result = result + key + ":" + map.get(key) + ";";
    }
    return result;
  }
}
