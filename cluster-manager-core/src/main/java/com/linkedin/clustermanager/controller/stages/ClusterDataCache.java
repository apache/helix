package com.linkedin.clustermanager.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.StateModelDefinition;

/**
 * Reads the data from the cluster using data accessor. This output ClusterData
 * which provides useful methods to search/lookup properties
 *
 * @author kgopalak
 *
 */
public class ClusterDataCache
{
  private final Map<String, LiveInstance> _liveInstanceMap = new HashMap<String, LiveInstance>();
  private final Map<String, IdealState> _idealStateMap = new HashMap<String, IdealState>();
  private final Map<String, StateModelDefinition> _stateModelDefMap = new HashMap<String, StateModelDefinition>();
  private final Map<String, InstanceConfig> _instanceConfigMap = new HashMap<String, InstanceConfig>();
  private final Map<String, Map<String, Map<String, CurrentState>>> _currentStateMap = new HashMap<String, Map<String, Map<String, CurrentState>>>();
  private final Map<String, Map<String, Message>> _messageMap = new HashMap<String, Map<String, Message>>();

  private static final Logger logger = Logger.getLogger(ClusterDataCache.class
      .getName());

//  private <T extends Object> Map<String, T> retrieve(
//      ClusterDataAccessor dataAccessor, PropertyType type, Class<T> clazz,
//      String... keys)
//  {
//    List<ZNRecord> instancePropertyList = dataAccessor.getChildValues(type,
//        keys);
//    Map<String, T> map = ZNRecordUtil.convertListToTypedMap(
//        instancePropertyList, clazz);
//    return map;
//  }
//
//  private <T extends Object> Map<String, T> retrieve(
//      ClusterDataAccessor dataAccessor, PropertyType type, Class<T> clazz)
//  {
//
//    List<ZNRecord> clusterPropertyList;
//    clusterPropertyList = dataAccessor.getChildValues(type);
//    Map<String, T> map = ZNRecordUtil.convertListToTypedMap(
//        clusterPropertyList, clazz);
//    return map;
//  }

  public boolean refresh(ClusterDataAccessor dataAccessor)
  {
    dataAccessor.<IdealState>refreshChildValues(_idealStateMap, IdealState.class, PropertyType.IDEALSTATES);
    dataAccessor.refreshChildValues(_liveInstanceMap, LiveInstance.class, PropertyType.LIVEINSTANCES);

    for (LiveInstance instance : _liveInstanceMap.values())
    {
      logger.trace("live instance: " + instance.getInstanceName() + " "
          + instance.getSessionId());
    }

    dataAccessor.<StateModelDefinition>refreshChildValues(_stateModelDefMap, StateModelDefinition.class, PropertyType.STATEMODELDEFS);
    dataAccessor.<InstanceConfig>refreshChildValues(_instanceConfigMap, InstanceConfig.class, PropertyType.CONFIGS);

    for (String instanceName : _liveInstanceMap.keySet())
    {
      if (!_messageMap.containsKey(instanceName))
      {
        _messageMap.put(instanceName, new HashMap<String, Message>());
      }
      dataAccessor.<Message>refreshChildValues(_messageMap.get(instanceName), Message.class, PropertyType.MESSAGES, instanceName);
    }

    for (String instanceName : _liveInstanceMap.keySet())
    {
      LiveInstance liveInstance = _liveInstanceMap.get(instanceName);
      String sessionId = liveInstance.getSessionId();
      if (!_currentStateMap.containsKey(instanceName))
      {
        _currentStateMap.put(instanceName,
            new HashMap<String, Map<String, CurrentState>>());
      }
      if (!_currentStateMap.get(instanceName).containsKey(sessionId))
      {
        _currentStateMap.get(instanceName)
            .put(sessionId, new HashMap<String, CurrentState>());
      }
      dataAccessor.<CurrentState>refreshChildValues(_currentStateMap.get(instanceName).get(sessionId),
                    CurrentState.class, PropertyType.CURRENTSTATES, instanceName, sessionId);
    }

    return true;
  }

  public Map<String, IdealState> getIdealStates()
  {
    return _idealStateMap;
  }

  public Map<String, LiveInstance> getLiveInstances()
  {
    return _liveInstanceMap;
  }

  public Map<String, CurrentState> getCurrentState(String instanceName,
      String clientSessionId)
  {
    return _currentStateMap.get(instanceName).get(clientSessionId);
  }

  public Map<String, Message> getMessages(String instanceName)
  {
    Map<String, Message> map = _messageMap.get(instanceName);
    if (map != null)
    {
      return map;
    }
    else
    {
      return Collections.emptyMap();
    }
  }

  public StateModelDefinition getStateModelDef(String stateModelDefRef)
  {

    return _stateModelDefMap.get(stateModelDefRef);
  }

  public IdealState getIdealState(String resourceGroupName)
  {
    return _idealStateMap.get(resourceGroupName);
  }

  public Map<String, InstanceConfig> getInstanceConfigMap()
  {
    return _instanceConfigMap;
  }

  public Set<String> getDisabledInstancesForResource(String resource)
  {
    Set<String> disabledInstancesSet = new HashSet<String>();
    for (String instance : _instanceConfigMap.keySet())
    {
      InstanceConfig config = _instanceConfigMap.get(instance);
      if (config.getInstanceEnabled() == false
          || config.getInstanceEnabledForResource(resource) == false)
      {
        disabledInstancesSet.add(instance);
      }
    }
    return disabledInstancesSet;
  }
}
