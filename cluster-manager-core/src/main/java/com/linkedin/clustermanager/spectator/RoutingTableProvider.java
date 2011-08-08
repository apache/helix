package com.linkedin.clustermanager.spectator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.spectator.RoutingTableProvider.StateUnitGroupInfo;
import com.linkedin.clustermanager.util.ZNRecordUtil;

public class RoutingTableProvider implements ExternalViewChangeListener
{
  private static final Logger logger = Logger
      .getLogger(RoutingTableProvider.class);
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

  /**
   * returns all instances for {stateUnitgroup} that are in a specific {state}
   * 
   * @param stateUnitGroup
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstances(String stateUnitGroup, String state)
  {
    Set<InstanceConfig> instanceSet = null;
    RoutingTable _routingTable = _routingTableRef.get();
    StateUnitGroupInfo stateUnitGroupInfo = _routingTable.get(stateUnitGroup);
    if (stateUnitGroupInfo != null)
    {
      instanceSet = stateUnitGroupInfo.getInstances(state);
    }
    if (instanceSet == null)
    {
      instanceSet = Collections.emptySet();
    }
    return instanceSet;
  }

  @Override
  public void onExternalViewChange(List<ZNRecord> externalViewList,
      NotificationContext changeContext)
  {
    refresh(externalViewList, changeContext);
  }

  private void refresh(List<ZNRecord> externalViewList,
      NotificationContext changeContext)
  {
    ClusterDataAccessor dataAccessor = changeContext.getManager()
        .getDataAccessor();
    List<ZNRecord> configList = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.CONFIGS);
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<String, InstanceConfig>();
    for (ZNRecord config : configList)
    {
      instanceConfigMap.put(config.getId(), new InstanceConfig(config));
    }
    RoutingTable newRoutingTable = new RoutingTable();
    if (externalViewList != null)
    {
      for (ZNRecord record : externalViewList)
      {
        String stateUnitGroupName = record.getId();
        Map<String, Map<String, String>> stateUnitKeysMap = record
            .getMapFields();
        for (String stateUnitKey : stateUnitKeysMap.keySet())
        {

          Map<String, String> stateUnitData = stateUnitKeysMap
              .get(stateUnitKey);
          for (String instanceName : stateUnitData.keySet())
          {
            String currentState = stateUnitData.get(instanceName);
            if (instanceConfigMap.containsKey(instanceName))
            {
              InstanceConfig instanceConfig = instanceConfigMap
                  .get(instanceName);
              newRoutingTable.addEntry(stateUnitGroupName, stateUnitKey,
                  currentState, instanceConfig);
            } else
            {
              logger.error("Invalid instance name." + instanceName
                  + " .Not found in /cluster/configs/. instanceName: ");
            }

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

    public void addEntry(String stateUnitGroupName, String stateUnitKey,
        String state, InstanceConfig config)
    {
      if (!groupInfoMap.containsKey(stateUnitGroupName))
      {
        groupInfoMap.put(stateUnitGroupName, new StateUnitGroupInfo());
      }
      StateUnitGroupInfo stateUnitGroupInfo = groupInfoMap
          .get(stateUnitGroupName);
      stateUnitGroupInfo.addEntry(stateUnitKey, state, config);

    }

    StateUnitGroupInfo get(String stateUnitGroup)
    {
      return groupInfoMap.get(stateUnitGroup);
    }

  }

  class StateUnitGroupInfo
  {
    // store StateUnitKeyInfo for each stateUnitKey
    HashMap<String, StateUnitKeyInfo> keyInfoMap;
    // stores the Set of Instances in a given state
    HashMap<String, Set<InstanceConfig>> stateInfoMap;

    public StateUnitGroupInfo()
    {
      keyInfoMap = new HashMap<String, RoutingTableProvider.StateUnitKeyInfo>();
      stateInfoMap = new HashMap<String, Set<InstanceConfig>>();
    }

    public void addEntry(String stateUnitKey, String state,
        InstanceConfig config)
    {
      // add
      if (!stateInfoMap.containsKey(state))
      {
        Comparator<InstanceConfig> comparator = new Comparator<InstanceConfig>()
        {

          @Override
          public int compare(InstanceConfig o1, InstanceConfig o2)
          {
            if (o1 == o2)
            {
              return 0;
            }
            if (o1 == null)
            {
              return -1;
            }
            if (o2 == null)
            {
              return 1;
            }

            int compareTo = o1.getHostName().compareTo(o2.getHostName());
            if (compareTo == 0)
            {
              return o1.getPort().compareTo(o2.getPort());
            } else
            {
              return compareTo;
            }

          }
        };
        stateInfoMap.put(state, new TreeSet<InstanceConfig>(comparator));
      }
      Set<InstanceConfig> set = stateInfoMap.get(state);
      set.add(config);

      if (!keyInfoMap.containsKey(stateUnitKey))
      {
        keyInfoMap.put(stateUnitKey, new StateUnitKeyInfo());
      }
      StateUnitKeyInfo stateUnitKeyInfo = keyInfoMap.get(stateUnitKey);
      stateUnitKeyInfo.addEntry(state, config);

    }

    public Set<InstanceConfig> getInstances(String state)
    {
      Set<InstanceConfig> instanceSet = stateInfoMap.get(state);
      return instanceSet;
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

    public void addEntry(String state, InstanceConfig config)
    {
      if (!stateInfoMap.containsKey(state))
      {
        stateInfoMap.put(state, new ArrayList<InstanceConfig>());
      }
      List<InstanceConfig> list = stateInfoMap.get(state);
      list.add(config);
    }

    List<InstanceConfig> get(String state)
    {
      return stateInfoMap.get(state);
    }
  }

}
