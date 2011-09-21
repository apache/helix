package com.linkedin.clustermanager.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.Criteria;

public class CriteriaEvaluator
{

  public List<Map<String, String>> evaluateCriteria(
      List<Map<String, String>> rows, Criteria recipientCriteria)
  {
    List<Map<String, String>> selected = new ArrayList<Map<String, String>>();
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
      if (!anyInstance
          && !instanceName.equalsIgnoreCase(row.get("instanceName")))
      {
        continue;
      }
      if (!anyResourceGroup
          && !resourceGroup.equalsIgnoreCase(row.get("resourceGroup")))
      {
        continue;
      }
      if (!anyResourceKey
          && !resourceKey.equalsIgnoreCase(row.get("resourceKey")))
      {
        continue;
      }
      if (!anyState && !state.equalsIgnoreCase(row.get("state")))
      {
        continue;
      }
      selected.add(row);
    }
    return selected;
  }

}
