package com.linkedin.clustermanager.spectator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.spectator.RoutingTableProvider.StateUnitGroupInfo;
import com.linkedin.clustermanager.util.ZNRecordUtil;

public class RoutingTableProvider implements ExternalViewChangeListener
{
  private AtomicReference<RoutingTable> _routingTableRef;

  // AtomicReference<V>

  public RoutingTableProvider()
  {
    _routingTableRef = new AtomicReference<RoutingTableProvider.RoutingTable>(
        new RoutingTable());

  }

  /**
   * returns the instances for {stateUnitgroup,stateUnitKey} pair that are in a
   * specific {state}
   * 
   * @param stateUnitGroup
   *          -
   * @param stateUnitKey
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstances(String stateUnitGroup,
      String stateUnitKey, String state)
  {
    List<InstanceConfig> instanceList = null;
    RoutingTable _routingTable = _routingTableRef.get();
    StateUnitGroupInfo stateUnitGroupInfo = _routingTable.get(stateUnitGroup);
    if (stateUnitGroupInfo != null)
    {
      StateUnitKeyInfo keyInfo = stateUnitGroupInfo.get(stateUnitKey);
      if (keyInfo != null)
      {
        instanceList = keyInfo.get(state);
      }
    }
    if (instanceList == null)
    {
      instanceList = Collections.emptyList();
    }
    return instanceList;
  }

  @Override
  public void onExternalViewChange(List<ZNRecord> externalViewList,
      NotificationContext changeContext)
  {
    refresh(externalViewList);
  }

  private void refresh(List<ZNRecord> externalViewList)
  {

    RoutingTable newRoutingTable = new RoutingTable();
    if (externalViewList != null)
    {
      for (ZNRecord record : externalViewList)
      {
        String stateUnitGroupName = record.getId();
        newRoutingTable.init(stateUnitGroupName);
        StateUnitGroupInfo groupInfo = newRoutingTable.get(stateUnitGroupName);
        Map<String, Map<String, String>> stateUnitKeysMap = record
            .getMapFields();
        for (String stateUnitKey : stateUnitKeysMap.keySet())
        {

          Map<String, String> stateUnitData = stateUnitKeysMap
              .get(stateUnitKey);
          for (String instanceName : stateUnitData.keySet())
          {
            groupInfo.init(stateUnitKey);
            StateUnitKeyInfo stateUnitKeyInfo = groupInfo.get(stateUnitKey);
            String currentState = stateUnitData.get(instanceName);
            stateUnitKeyInfo.init(currentState);
            List<InstanceConfig> list = stateUnitKeyInfo.get(currentState);
            InstanceConfig instanceConfig = new InstanceConfig();
            String[] split = instanceName.split("_");
            if (split.length == 2)
            {
              instanceConfig.setHostName(split[0]);
              instanceConfig.setPort(split[1]);
            }
            list.add(instanceConfig);
          }
        }
      }
    }
    _routingTableRef.set(newRoutingTable);
  }

  private void refreshOptimized(List<ZNRecord> externalViewList)
  {

    if (externalViewList == null)
    {
      return;
    }

    Set<String> prevStateUnitGroups = new HashSet<String>();
    Map<String, ZNRecord> stateUnitGroupsMap = ZNRecordUtil
        .convertListToMap(externalViewList);

    RoutingTable routingTable = _routingTableRef.get();
    Map<String, StateUnitGroupInfo> groupInfoMap = routingTable.groupInfoMap;
    for (String stateUnitGroup : groupInfoMap.keySet())
    {
      prevStateUnitGroups.add(stateUnitGroup);
    }

    // get the union of state unit groups from current and old
    Set<String> combined = new HashSet<String>();
    combined.addAll(groupInfoMap.keySet());
    combined.addAll(stateUnitGroupsMap.keySet());

    for (String stateUnitGroup : combined)
    {

      if (!groupInfoMap.containsKey(stateUnitGroup)
          && stateUnitGroupsMap.containsKey(stateUnitGroup))
      {
        // handle addition
        StateUnitGroupInfo value = new StateUnitGroupInfo();
        groupInfoMap.put(stateUnitGroup, value);
      } else if (!groupInfoMap.containsKey(stateUnitGroup)
          && stateUnitGroupsMap.containsKey(stateUnitGroup))
      {
        // handle deletion
        groupInfoMap.remove(stateUnitGroup);
      } else if (groupInfoMap.containsKey(stateUnitGroup)
          && stateUnitGroupsMap.containsKey(stateUnitGroup))
      {
        // handle update
        StateUnitGroupInfo stateUnitGroupInfo = groupInfoMap
            .get(stateUnitGroup);
        // stateUnitGroupInfo.refresh(stateUnitGroupsMap);
      }

    }

  }

  class RoutingTable
  {
    private HashMap<String, StateUnitGroupInfo> groupInfoMap;

    public RoutingTable()
    {
      groupInfoMap = new HashMap<String, RoutingTableProvider.StateUnitGroupInfo>();
    }

    public void init(String stateUnitGroupName)
    {
      if (!groupInfoMap.containsKey(stateUnitGroupName))
      {
        groupInfoMap.put(stateUnitGroupName, new StateUnitGroupInfo());
      }
    }

    StateUnitGroupInfo get(String stateUnitGroup)
    {
      return groupInfoMap.get(stateUnitGroup);
    }

  }

  class StateUnitGroupInfo
  {
    HashMap<String, StateUnitKeyInfo> keyInfoMap;

    public StateUnitGroupInfo()
    {
      keyInfoMap = new HashMap<String, RoutingTableProvider.StateUnitKeyInfo>();
    }

    void init(String stateUnitKey)
    {
      if (!keyInfoMap.containsKey(stateUnitKey))
      {
        keyInfoMap.put(stateUnitKey, new StateUnitKeyInfo());
      }
    }

    StateUnitKeyInfo get(String stateUnitKey)
    {
      return keyInfoMap.get(stateUnitKey);
    }
  }

  class StateUnitKeyInfo
  {
    HashMap<String, List<InstanceConfig>> stateInfoMap;

    public StateUnitKeyInfo()
    {
      stateInfoMap = new HashMap<String, List<InstanceConfig>>();
    }

    void init(String state)
    {
      if (!stateInfoMap.containsKey(state))
      {
        stateInfoMap.put(state, new ArrayList<InstanceConfig>());
      }
    }

    List<InstanceConfig> get(String state)
    {
      return stateInfoMap.get(state);
    }
  }

}
