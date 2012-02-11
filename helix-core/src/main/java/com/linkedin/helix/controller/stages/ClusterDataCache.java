package com.linkedin.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.helix.CMConstants.StateModelToken;
import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.model.AlertStatus;
import com.linkedin.helix.model.Alerts;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.PersistentStats;
import com.linkedin.helix.model.StateModelDefinition;

/**
 * Reads the data from the cluster using data accessor. This output ClusterData
 * which provides useful methods to search/lookup properties
 *
 * @author kgopalak
 *
 */
public class ClusterDataCache
{

  Map<String, LiveInstance> _liveInstanceMap;
  Map<String, IdealState> _idealStateMap;
  Map<String, StateModelDefinition> _stateModelDefMap;
  Map<String, InstanceConfig> _instanceConfigMap;
  Map<String, Map<String, Map<String, CurrentState>>> _currentStateMap;
  Map<String, Map<String, Message>> _messageMap;
  
  Map<String, Map<String, HealthStat>> _healthStatMap;
  private HealthStat _globalStats;  //DON'T THINK I WILL USE THIS ANYMORE
  private PersistentStats _persistentStats;
  private Alerts _alerts;
  private AlertStatus _alertStatus;

  private static final Logger LOG = Logger.getLogger(ClusterDataCache.class.getName());

  public boolean refresh(ClusterDataAccessor accessor)
  {
    _idealStateMap = accessor.getChildValuesMap(IdealState.class, 
                                                PropertyType.IDEALSTATES);  
    _liveInstanceMap = accessor.getChildValuesMap(LiveInstance.class, 
                                                  PropertyType.LIVEINSTANCES);

    for (LiveInstance instance : _liveInstanceMap.values())
    {
      LOG.trace("live instance: " + instance.getInstanceName() + " "
          + instance.getSessionId());
    }

    _stateModelDefMap = accessor.getChildValuesMap(StateModelDefinition.class, 
                                                   PropertyType.STATEMODELDEFS);
    _instanceConfigMap = accessor.getChildValuesMap(InstanceConfig.class, 
                                                    PropertyType.CONFIGS);
    Map<String, Map<String, Message>> msgMap = new HashMap<String, Map<String, Message>>();
    for (String instanceName : _liveInstanceMap.keySet())
    {
      msgMap.put(instanceName, accessor.getChildValuesMap(Message.class, 
                                                          PropertyType.MESSAGES, 
                                                          instanceName));
    }
    _messageMap = Collections.unmodifiableMap(msgMap);

    Map<String, Map<String, Map<String, CurrentState>>> allCurStateMap
      = new HashMap<String, Map<String, Map<String, CurrentState>>>();
    for (String instanceName : _liveInstanceMap.keySet())
    {
      LiveInstance liveInstance = _liveInstanceMap.get(instanceName);
      String sessionId = liveInstance.getSessionId();
      if (!allCurStateMap.containsKey(instanceName))
      {
        allCurStateMap.put(instanceName, new HashMap<String, Map<String, CurrentState>>());
      }
      Map<String, Map<String, CurrentState>> curStateMap = allCurStateMap.get(instanceName);
      curStateMap.put(sessionId, accessor.getChildValuesMap(CurrentState.class, 
                                                            PropertyType.CURRENTSTATES, 
                                                            instanceName, 
                                                            sessionId));
    }
    
    for (String instance : allCurStateMap.keySet())
    {
      allCurStateMap.put(instance, Collections.unmodifiableMap(allCurStateMap.get(instance)));
    }
    _currentStateMap = Collections.unmodifiableMap(allCurStateMap);

    Map<String, Map<String, HealthStat>> hsMap = new HashMap<String, Map<String, HealthStat>>();
    for (String instanceName : _liveInstanceMap.keySet())
    {
    	//xxx clearly getting znodes for the instance here...so get the timestamp!
    	hsMap.put(instanceName, accessor.getChildValuesMap(HealthStat.class, 
    	                                                   PropertyType.HEALTHREPORT,
    	                                                   instanceName));
    }
    _healthStatMap = Collections.unmodifiableMap(hsMap);
    _persistentStats = accessor.getProperty(PersistentStats.class,
                                            PropertyType.PERSISTENTSTATS);
    _alerts = accessor.getProperty(Alerts.class, PropertyType.ALERTS);
    _alertStatus = accessor.getProperty(AlertStatus.class, PropertyType.ALERT_STATUS);

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

  public HealthStat getGlobalStats()
  {
	  return _globalStats;
  }

  public PersistentStats getPersistentStats() 
  {
	  return _persistentStats;
  }
  
  public Alerts getAlerts()
  {
	  return _alerts;
  }
  
  public AlertStatus getAlertStatus()
  {
	  return _alertStatus;
  }
  
  public Map<String, HealthStat> getHealthStats(String instanceName)
  {
	  Map<String, HealthStat> map = _healthStatMap.get(instanceName);
    if (map != null)
    {
      return map;
    } else
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

  public Set<String> getDisabledInstancesForPartition(String partition)
  {
    Set<String> disabledInstancesSet = new HashSet<String>();
    for (String instance : _instanceConfigMap.keySet())
    {
      InstanceConfig config = _instanceConfigMap.get(instance);
      if (config.getInstanceEnabled() == false
          || config.getInstanceEnabledForPartition(partition) == false)
      {
        disabledInstancesSet.add(instance);
      }
    }
    return disabledInstancesSet;
  }
  
  public int getReplicas(String resourceGroup)
  {
    String replicasStr = _idealStateMap.get(resourceGroup).getReplicas();
    int replicas;
    if (replicasStr.equals(StateModelToken.ANY_LIVEINSTANCE.toString()))
    {
      replicas = _liveInstanceMap.size();
    }
    else
    {
      try
      {
        replicas = Integer.parseInt(replicasStr);
      } catch (Exception e)
      {
        replicas = -1;
      }
    }
    return replicas;
  }
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("liveInstaceMap:" + _liveInstanceMap).append("\n");
    sb.append("idealStateMap:" + _idealStateMap).append("\n");
    sb.append("stateModelDefMap:" + _stateModelDefMap).append("\n");
    sb.append("instanceConfigMap:" + _instanceConfigMap).append("\n");
    sb.append("messageMap:" + _messageMap).append("\n");
    
    return sb.toString();
  }
}
