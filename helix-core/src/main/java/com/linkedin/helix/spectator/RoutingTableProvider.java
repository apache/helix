package com.linkedin.helix.spectator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.InstanceConfig;

public class RoutingTableProvider implements ExternalViewChangeListener
{
  private static final Logger logger = Logger.getLogger(RoutingTableProvider.class);
  private final AtomicReference<RoutingTable> _routingTableRef;

  public RoutingTableProvider()
  {
    _routingTableRef = new AtomicReference<RoutingTableProvider.RoutingTable>(new RoutingTable());

  }

  /**
   * returns the instances for {resource,partition} pair that are in a specific
   * {state}
   * 
   * @param resourceName
   *          -
   * @param partitionName
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public List<InstanceConfig> getInstances(String resourceName, String partitionName, String state)
  {
    List<InstanceConfig> instanceList = null;
    RoutingTable _routingTable = _routingTableRef.get();
    ResourceInfo resourceInfo = _routingTable.get(resourceName);
    if (resourceInfo != null)
    {
      PartitionInfo keyInfo = resourceInfo.get(partitionName);
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
   * returns all instances for {resource} that are in a specific {state}
   * 
   * @param resource
   * @param state
   * @return empty list if there is no instance in a given state
   */
  public Set<InstanceConfig> getInstances(String resource, String state)
  {
    Set<InstanceConfig> instanceSet = null;
    RoutingTable routingTable = _routingTableRef.get();
    ResourceInfo resourceInfo = routingTable.get(resource);
    if (resourceInfo != null)
    {
      instanceSet = resourceInfo.getInstances(state);
    }
    if (instanceSet == null)
    {
      instanceSet = Collections.emptySet();
    }
    return instanceSet;
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext)
  {
    // session has expired clean up the routing table
    if (changeContext.getType() == NotificationContext.Type.FINALIZE)
    {
      logger.info("Resetting the routing table. ");
      RoutingTable newRoutingTable = new RoutingTable();
      _routingTableRef.set(newRoutingTable);
      return;
    }
    refresh(externalViewList, changeContext);
  }

  private void refresh(List<ExternalView> externalViewList, NotificationContext changeContext)
  {
    DataAccessor dataAccessor = changeContext.getManager().getDataAccessor();
    List<InstanceConfig> configList = dataAccessor.getChildValues(InstanceConfig.class,
        PropertyType.CONFIGS, ConfigScopeProperty.PARTICIPANT.toString());
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<String, InstanceConfig>();
    for (InstanceConfig config : configList)
    {
      instanceConfigMap.put(config.getId(), config);
    }
    RoutingTable newRoutingTable = new RoutingTable();
    if (externalViewList != null)
    {
      for (ExternalView extView : externalViewList)
      {
        String resourceName = extView.getId();
        for (String partitionName : extView.getPartitionSet())
        {
          Map<String, String> stateMap = extView.getStateMap(partitionName);
          for (String instanceName : stateMap.keySet())
          {
            String currentState = stateMap.get(instanceName);
            if (instanceConfigMap.containsKey(instanceName))
            {
              InstanceConfig instanceConfig = instanceConfigMap.get(instanceName);
              newRoutingTable.addEntry(resourceName, partitionName, currentState, instanceConfig);
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

  class RoutingTable
  {
    private final HashMap<String, ResourceInfo> resourceInfoMap;

    public RoutingTable()
    {
      resourceInfoMap = new HashMap<String, RoutingTableProvider.ResourceInfo>();
    }

    public void addEntry(String resourceName, String partitionName, String state,
        InstanceConfig config)
    {
      if (!resourceInfoMap.containsKey(resourceName))
      {
        resourceInfoMap.put(resourceName, new ResourceInfo());
      }
      ResourceInfo resourceInfo = resourceInfoMap.get(resourceName);
      resourceInfo.addEntry(partitionName, state, config);

    }

    ResourceInfo get(String resourceName)
    {
      return resourceInfoMap.get(resourceName);
    }

  }

  class ResourceInfo
  {
    // store PartitionInfo for each partition
    HashMap<String, PartitionInfo> partitionInfoMap;
    // stores the Set of Instances in a given state
    HashMap<String, Set<InstanceConfig>> stateInfoMap;

    public ResourceInfo()
    {
      partitionInfoMap = new HashMap<String, RoutingTableProvider.PartitionInfo>();
      stateInfoMap = new HashMap<String, Set<InstanceConfig>>();
    }

    public void addEntry(String stateUnitKey, String state, InstanceConfig config)
    {
      // add
      if (!stateInfoMap.containsKey(state))
      {
        Comparator<InstanceConfig> comparator = new Comparator<InstanceConfig>() {

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

      if (!partitionInfoMap.containsKey(stateUnitKey))
      {
        partitionInfoMap.put(stateUnitKey, new PartitionInfo());
      }
      PartitionInfo stateUnitKeyInfo = partitionInfoMap.get(stateUnitKey);
      stateUnitKeyInfo.addEntry(state, config);

    }

    public Set<InstanceConfig> getInstances(String state)
    {
      Set<InstanceConfig> instanceSet = stateInfoMap.get(state);
      return instanceSet;
    }

    PartitionInfo get(String stateUnitKey)
    {
      return partitionInfoMap.get(stateUnitKey);
    }
  }

  class PartitionInfo
  {
    HashMap<String, List<InstanceConfig>> stateInfoMap;

    public PartitionInfo()
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
