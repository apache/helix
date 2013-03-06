package org.apache.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.apache.helix.PropertyType.ALERTS;
import static org.apache.helix.PropertyType.ALERT_HISTORY;
import static org.apache.helix.PropertyType.ALERT_STATUS;
import static org.apache.helix.PropertyType.CONFIGS;
import static org.apache.helix.PropertyType.CONTROLLER;
import static org.apache.helix.PropertyType.CURRENTSTATES;
import static org.apache.helix.PropertyType.ERRORS;
import static org.apache.helix.PropertyType.ERRORS_CONTROLLER;
import static org.apache.helix.PropertyType.EXTERNALVIEW;
import static org.apache.helix.PropertyType.HEALTHREPORT;
import static org.apache.helix.PropertyType.HISTORY;
import static org.apache.helix.PropertyType.IDEALSTATES;
import static org.apache.helix.PropertyType.LEADER;
import static org.apache.helix.PropertyType.LIVEINSTANCES;
import static org.apache.helix.PropertyType.MESSAGES;
import static org.apache.helix.PropertyType.MESSAGES_CONTROLLER;
import static org.apache.helix.PropertyType.PAUSE;
import static org.apache.helix.PropertyType.PERSISTENTSTATS;
import static org.apache.helix.PropertyType.STATEMODELDEFS;
import static org.apache.helix.PropertyType.STATUSUPDATES;
import static org.apache.helix.PropertyType.STATUSUPDATES_CONTROLLER;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ConfigScope.ConfigScopeProperty;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.AlertHistory;
import org.apache.helix.model.AlertStatus;
import org.apache.helix.model.Alerts;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Error;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HealthStat;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderHistory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.PersistentStats;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.StatusUpdate;
import org.apache.log4j.Logger;


public class PropertyKey
{
  private static Logger          LOG = Logger.getLogger(PropertyKey.class);
  public PropertyType            _type;
  private final String[]         _params;
  Class<? extends HelixProperty> _typeClazz;

  //if type is CONFIGS, set configScope; otherwise null
  ConfigScopeProperty _configScope;

  public PropertyKey(PropertyType type, Class<? extends HelixProperty> typeClazz, 
		  String... params)
  {
    this(type, null, typeClazz, params);
  }
  
  public PropertyKey(PropertyType type,
		  			 ConfigScopeProperty configScope,
                     Class<? extends HelixProperty> typeClazz,
                     String... params)
  {
    _type = type;
    if (params == null || params.length == 0 || Arrays.asList(params).contains(null))
    {
      throw new IllegalArgumentException("params cannot be null");
    }

    _params = params;
    _typeClazz = typeClazz;
    
    _configScope = configScope;
  }

  @Override
  public int hashCode()
  {
    return super.hashCode();
  }

  @Override
  public String toString() {
	  return getPath();
  }
  
  public String getPath()
  {
    String clusterName = _params[0];
    String[] subKeys = Arrays.copyOfRange(_params, 1, _params.length);
    String path = PropertyPathConfig.getPath(_type, clusterName, subKeys);
    if (path == null)
    {
      LOG.error("Invalid property key with type:" + _type + "subKeys:"
          + Arrays.toString(_params));
    }
    return path;
  }

  public static class Builder
  {
    private final String _clusterName;

    public Builder(String clusterName)
    {
      _clusterName = clusterName;
    }

    public PropertyKey idealStates()
    {
      return new PropertyKey(IDEALSTATES, IdealState.class, _clusterName);
    }

    public PropertyKey idealStates(String resourceName)
    {
      return new PropertyKey(IDEALSTATES, IdealState.class, _clusterName, resourceName);
    }

    public PropertyKey stateModelDefs()
    {
      return new PropertyKey(STATEMODELDEFS, StateModelDefinition.class, _clusterName);
    }

    public PropertyKey stateModelDef(String stateModelName)
    {
      return new PropertyKey(STATEMODELDEFS,
                             StateModelDefinition.class,
                             _clusterName,
                             stateModelName);
    }

    public PropertyKey clusterConfigs()
    {
      return new PropertyKey(CONFIGS,
    		  				 ConfigScopeProperty.CLUSTER,
                             HelixProperty.class,
                             _clusterName,
                             ConfigScopeProperty.CLUSTER.toString());
    }
    
    public PropertyKey clusterConfig()
    {
    	return new PropertyKey(CONFIGS,
 				 ConfigScopeProperty.CLUSTER,
                HelixProperty.class,
                _clusterName,
                ConfigScopeProperty.CLUSTER.toString(),
                _clusterName);    	
    }

    public PropertyKey instanceConfigs()
    {
        return new PropertyKey(CONFIGS,
 				 ConfigScopeProperty.PARTICIPANT,
                InstanceConfig.class,
                _clusterName,
                ConfigScopeProperty.PARTICIPANT.toString());        
    }

    public PropertyKey instanceConfig(String instanceName)
    {
      return new PropertyKey(CONFIGS,
    		  	ConfigScopeProperty.PARTICIPANT,
                             InstanceConfig.class,
                             _clusterName,
                             ConfigScopeProperty.PARTICIPANT.toString(),
                             instanceName);
    }

    public PropertyKey resourceConfigs()
    {
      return new PropertyKey(CONFIGS,
				 			 ConfigScopeProperty.RESOURCE,
    		  				 HelixProperty.class,
                             _clusterName,
                             ConfigScopeProperty.RESOURCE.toString());
    }
    
    public PropertyKey resourceConfig(String resourceName)
    {
      return new PropertyKey(CONFIGS,
    		  				ConfigScopeProperty.RESOURCE,
    		  				HelixProperty.class,
                             _clusterName,
                             ConfigScopeProperty.RESOURCE.toString(),
                             resourceName);
    }

    public PropertyKey partitionConfig(String resourceName, String partitionName)
    {
      return new PropertyKey(CONFIGS,
    		  				ConfigScopeProperty.RESOURCE,
    		  				HelixProperty.class,
                             _clusterName,
                             ConfigScopeProperty.RESOURCE.toString(),
                             resourceName);
    }

    public PropertyKey partitionConfig(String instanceName,
                                       String resourceName,
                                       String partitionName)
    {
      return new PropertyKey(CONFIGS,
    		  				ConfigScopeProperty.RESOURCE,
				 			 HelixProperty.class,
				 			 _clusterName,
                             ConfigScopeProperty.RESOURCE.toString(),
                             resourceName);
    }

    public PropertyKey constraints()
    {
      return new PropertyKey(CONFIGS,
                             ClusterConstraints.class,
                             _clusterName,
                             ConfigScopeProperty.CONSTRAINT.toString());
    }

    public PropertyKey constraint(String constraintType)
    {
      return new PropertyKey(CONFIGS,
                             ClusterConstraints.class,
                             _clusterName,
                             ConfigScopeProperty.CONSTRAINT.toString(),
                             constraintType);
    }

    public PropertyKey liveInstances()
    {
      return new PropertyKey(LIVEINSTANCES, LiveInstance.class, _clusterName);
    }

    public PropertyKey liveInstance(String instanceName)
    {
      return new PropertyKey(LIVEINSTANCES,
                             LiveInstance.class,
                             _clusterName,
                             instanceName);
    }

    public PropertyKey instances()
    {
      return new PropertyKey(CONFIGS, null, _clusterName);
    }

    public PropertyKey messages(String instanceName)
    {
      return new PropertyKey(MESSAGES, Message.class, _clusterName, instanceName);
    }

    public PropertyKey message(String instanceName, String messageId)
    {
      return new PropertyKey(MESSAGES,
                             Message.class,
                             _clusterName,
                             instanceName,
                             messageId);
    }

    public PropertyKey sessions(String instanceName)
    {
      return new PropertyKey(CURRENTSTATES,
                             CurrentState.class,
                             _clusterName,
                             instanceName);
    }

    public PropertyKey currentStates(String instanceName, String sessionId)
    {
      return new PropertyKey(CURRENTSTATES,
                             CurrentState.class,
                             _clusterName,
                             instanceName,
                             sessionId);
    }

    public PropertyKey currentState(String instanceName,
                                    String sessionId,
                                    String resourceName)
    {
      return new PropertyKey(CURRENTSTATES,
                             CurrentState.class,
                             _clusterName,
                             instanceName,
                             sessionId,
                             resourceName);
    }

    public PropertyKey currentState(String instanceName,
                                    String sessionId,
                                    String resourceName,
                                    String bucketName)
    {
      if (bucketName == null)
      {
        return new PropertyKey(CURRENTSTATES,
                               CurrentState.class,
                               _clusterName,
                               instanceName,
                               sessionId,
                               resourceName);

      }
      else
      {
        return new PropertyKey(CURRENTSTATES,
                               CurrentState.class,
                               _clusterName,
                               instanceName,
                               sessionId,
                               resourceName,
                               bucketName);
      }
    }

    public PropertyKey stateTransitionStatus(String instanceName,
                                             String sessionId,
                                             String resourceName,
                                             String partitionName)
    {
      return new PropertyKey(STATUSUPDATES,
                             StatusUpdate.class,
                             _clusterName,
                             instanceName,
                             sessionId,
                             resourceName,
                             partitionName);
    }

    public PropertyKey stateTransitionStatus(String instanceName,
                                             String sessionId,
                                             String resourceName)
    {
      return new PropertyKey(STATUSUPDATES,
                             StatusUpdate.class,
                             _clusterName,
                             instanceName,
                             sessionId,
                             resourceName);
    }

    public PropertyKey stateTransitionStatus(String instanceName, String sessionId)
    {
      return new PropertyKey(STATUSUPDATES,
                             StatusUpdate.class,
                             _clusterName,
                             instanceName,
                             sessionId);
    }

    /**
     * Used to get status update for a NON STATE TRANSITION type
     * 
     * @param instanceName
     * @param sessionId
     * @param msgType
     * @param msgId
     * @return
     */
    public PropertyKey taskStatus(String instanceName,
                                  String sessionId,
                                  String msgType,
                                  String msgId)
    {
      return new PropertyKey(STATUSUPDATES,
                             StatusUpdate.class,
                             _clusterName,
                             instanceName,
                             sessionId,
                             msgType,
                             msgId);
    }

    public PropertyKey stateTransitionError(String instanceName,
                                            String sessionId,
                                            String resourceName,
                                            String partitionName)
    {
      return new PropertyKey(ERRORS,
                             Error.class,
                             _clusterName,
                             instanceName,
                             sessionId,
                             resourceName,
                             partitionName);
    }

    public PropertyKey stateTransitionErrors(String instanceName,
                                             String sessionId,
                                             String resourceName)
    {
      return new PropertyKey(ERRORS,
                             Error.class,
                             _clusterName,
                             instanceName,
                             sessionId,
                             resourceName);
    }

    /**
     * Used to get status update for a NON STATE TRANSITION type
     * 
     * @param instanceName
     * @param sessionId
     * @param msgType
     * @param msgId
     * @return
     */
    public PropertyKey taskError(String instanceName,
                                 String sessionId,
                                 String msgType,
                                 String msgId)
    {
      return new PropertyKey(ERRORS,
                             null,
                             _clusterName,
                             instanceName,
                             sessionId,
                             msgType,
                             msgId);
    }

    public PropertyKey externalViews()
    {
      return new PropertyKey(EXTERNALVIEW, ExternalView.class, _clusterName);
    }

    public PropertyKey externalView(String resourceName)
    {
      return new PropertyKey(EXTERNALVIEW, ExternalView.class, _clusterName, resourceName);
    }

    public PropertyKey controller()
    {
      return new PropertyKey(CONTROLLER, null, _clusterName);
    }

    public PropertyKey controllerTaskErrors()
    {
      return new PropertyKey(ERRORS_CONTROLLER, StatusUpdate.class, _clusterName);
    }

    public PropertyKey controllerTaskError(String errorId)
    {
      return new PropertyKey(ERRORS_CONTROLLER, Error.class, _clusterName, errorId);
    }

    public PropertyKey controllerTaskStatuses(String subPath)
    {
      return new PropertyKey(STATUSUPDATES_CONTROLLER,
                             StatusUpdate.class,
                             _clusterName,
                             subPath);
    }

    public PropertyKey controllerTaskStatus(String subPath, String recordName)
    {
      return new PropertyKey(STATUSUPDATES_CONTROLLER,
                             StatusUpdate.class,
                             _clusterName,
                             subPath,
                             recordName);
    }

    public PropertyKey controllerMessages()
    {
      return new PropertyKey(MESSAGES_CONTROLLER, Message.class, _clusterName);
    }

    public PropertyKey controllerMessage(String msgId)
    {
      return new PropertyKey(MESSAGES_CONTROLLER, Message.class, _clusterName, msgId);
    }

    public PropertyKey controllerLeaderHistory()
    {
      return new PropertyKey(HISTORY, LeaderHistory.class, _clusterName);
    }

    public PropertyKey controllerLeader()
    {
      return new PropertyKey(LEADER, LiveInstance.class, _clusterName);
    }

    public PropertyKey pause()
    {
      return new PropertyKey(PAUSE, PauseSignal.class, _clusterName);
    }

    public PropertyKey persistantStat()
    {
      return new PropertyKey(PERSISTENTSTATS, PersistentStats.class, _clusterName);
    }

    public PropertyKey alerts()
    {
      return new PropertyKey(ALERTS, Alerts.class, _clusterName);
    }

    public PropertyKey alertStatus()
    {
      return new PropertyKey(ALERT_STATUS, AlertStatus.class, _clusterName);
    }

    public PropertyKey alertHistory()
    {
      return new PropertyKey(ALERT_HISTORY, AlertHistory.class, _clusterName);
    }

    public PropertyKey healthReport(String instanceName, String id)
    {
      return new PropertyKey(HEALTHREPORT,
                             HealthStat.class,
                             _clusterName,
                             instanceName,
                             id);
    }

    public PropertyKey healthReports(String instanceName)
    {
      return new PropertyKey(HEALTHREPORT, HealthStat.class, _clusterName, instanceName);
    }

  }

  public PropertyType getType()
  {
    return _type;
  }

  public String[] getParams()
  {
    return _params;
  }

  public Class<? extends HelixProperty> getTypeClass()
  {
    return _typeClazz;
  }

  public ConfigScopeProperty getConfigScope()
  {
	  return _configScope;
  }
  
}
