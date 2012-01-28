package com.linkedin.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;
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
  final Map<String, Map<String, Map<String, CurrentState>>> _currentStateMap = new HashMap<String, Map<String, Map<String, CurrentState>>>();
  final Map<String, Map<String, Message>> _messageMap = new HashMap<String, Map<String, Message>>();
  final Map<String, Map<String, HealthStat>> _healthStatMap = new HashMap<String, Map<String, HealthStat>>();
  private HealthStat _globalStats;  //DON'T THINK I WILL USE THIS ANYMORE
  private PersistentStats _persistentStats;
  private Alerts _alerts;

  private static final Logger logger = Logger.getLogger(ClusterDataCache.class.getName());

  public boolean refresh(ClusterDataAccessor dataAccessor)
  {
    _idealStateMap = ZNRecordDecorator
        .convertListToMap(dataAccessor.getChildValues(IdealState.class,
                                                                PropertyType.IDEALSTATES));
    _liveInstanceMap = ZNRecordDecorator
        .convertListToMap(dataAccessor.getChildValues(LiveInstance.class,
                                                                PropertyType.LIVEINSTANCES));


    for (LiveInstance instance : _liveInstanceMap.values())
    {
      logger.trace("live instance: " + instance.getInstanceName() + " "
          + instance.getSessionId());
    }

    _stateModelDefMap = ZNRecordDecorator
        .convertListToMap(dataAccessor.getChildValues(StateModelDefinition.class,
                                                                PropertyType.STATEMODELDEFS));
    _instanceConfigMap = ZNRecordDecorator
        .convertListToMap(dataAccessor.getChildValues(InstanceConfig.class,
                                                                PropertyType.CONFIGS));

    for (String instanceName : _liveInstanceMap.keySet())
    {
      _messageMap.put(instanceName, ZNRecordDecorator
                      .convertListToMap(dataAccessor.getChildValues(Message.class,
                                                                              PropertyType.MESSAGES,
                                                                              instanceName)));
    }

    for (String instanceName : _liveInstanceMap.keySet())
    {
      LiveInstance liveInstance = _liveInstanceMap.get(instanceName);
      String sessionId = liveInstance.getSessionId();
      if (!_currentStateMap.containsKey(instanceName))
      {
        _currentStateMap.put(instanceName, new HashMap<String, Map<String, CurrentState>>());
      }
        _currentStateMap.get(instanceName).put(sessionId, ZNRecordDecorator
                        .convertListToMap(dataAccessor.getChildValues(CurrentState.class,
                                                                                PropertyType.CURRENTSTATES,
                                                                                instanceName,
                                                                                sessionId)));
    }

    for (String instanceName : _liveInstanceMap.keySet())
    {
    	
    	 _healthStatMap.put(instanceName, ZNRecordDecorator
                 .convertListToMap(dataAccessor.getChildValues(HealthStat.class,
                                                                         PropertyType.HEALTHREPORT,
                                                                         instanceName)));
    }

    try {
    	
    	ZNRecordDecorator statsRec = dataAccessor.getProperty(PersistentStats.class, 
    														  PropertyType.PERSISTENTSTATS);
    	
    	if (statsRec != null) {
    		_persistentStats = new PersistentStats(statsRec.getRecord());
    	}
    } catch (Exception e) {
    	logger.debug("No persistent stats found: "+e);
    }
    

    try {
    	ZNRecordDecorator alertsRec = dataAccessor.getProperty(Alerts.class,
    														   PropertyType.ALERTS);
    	if (alertsRec != null) {
    		_alerts = new Alerts(alertsRec.getRecord());
    	}
    } catch (Exception e) {
    	logger.debug("No alerts found: "+e);
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
  
  public Map<String, HealthStat> getHealthStats(String instanceName)
  {
	  Map<String, HealthStat> list = _healthStatMap.get(instanceName);
	  if (list != null)
	    {
	      return list;
	    } else
	    {
	      //return Collections.emptyList();
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
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("liveInstaceMap:" + _liveInstanceMap).append("/n");
    sb.append("idealStateMap:" + _idealStateMap).append("/n");
    sb.append("stateModelDefMap:" + _stateModelDefMap).append("/n");
    sb.append("instanceConfigMap:" + _instanceConfigMap).append("/n");
    sb.append("messageMap:" + _messageMap).append("\n");
    
    return sb.toString();
  }
}
