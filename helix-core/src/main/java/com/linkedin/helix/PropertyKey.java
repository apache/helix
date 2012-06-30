package com.linkedin.helix;

import static com.linkedin.helix.PropertyType.ALERTS;
import static com.linkedin.helix.PropertyType.ALERT_HISTORY;
import static com.linkedin.helix.PropertyType.ALERT_STATUS;
import static com.linkedin.helix.PropertyType.CONFIGS;
import static com.linkedin.helix.PropertyType.CURRENTSTATES;
import static com.linkedin.helix.PropertyType.ERRORS;
import static com.linkedin.helix.PropertyType.ERRORS_CONTROLLER;
import static com.linkedin.helix.PropertyType.EXTERNALVIEW;
import static com.linkedin.helix.PropertyType.HEALTHREPORT;
import static com.linkedin.helix.PropertyType.HISTORY;
import static com.linkedin.helix.PropertyType.CONTROLLER;
import static com.linkedin.helix.PropertyType.IDEALSTATES;
import static com.linkedin.helix.PropertyType.LEADER;
import static com.linkedin.helix.PropertyType.LIVEINSTANCES;
import static com.linkedin.helix.PropertyType.MESSAGES;
import static com.linkedin.helix.PropertyType.MESSAGES_CONTROLLER;
import static com.linkedin.helix.PropertyType.PAUSE;
import static com.linkedin.helix.PropertyType.PERSISTENTSTATS;
import static com.linkedin.helix.PropertyType.STATEMODELDEFS;
import static com.linkedin.helix.PropertyType.STATUSUPDATES;
import static com.linkedin.helix.PropertyType.STATUSUPDATES_CONTROLLER;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.AlertHistory;
import com.linkedin.helix.model.AlertStatus;
import com.linkedin.helix.model.Alerts;
import com.linkedin.helix.model.ClusterConstraints;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.Error;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LeaderHistory;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.PauseSignal;
import com.linkedin.helix.model.PersistentStats;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.model.StatusUpdate;

public class PropertyKey
{
  private static Logger LOG = Logger.getLogger(PropertyKey.class);
  public PropertyType _type;
  private final String[] _params;
  Class<? extends HelixProperty> _typeClazz;

  public PropertyKey(PropertyType type,
      Class<? extends HelixProperty> typeClazz, String... params) 
  {
    _type = type;
    if(params==null || params.length==0 || Arrays.asList(params).contains(null)){
      throw new IllegalArgumentException("params cannot be null");
    }
    
    _params = params;
    _typeClazz = typeClazz;
  }

  @Override
  public int hashCode()
  {
    return super.hashCode();
  }

  public String getPath()
  {
    String clusterName = _params[0];
    String[] subKeys = Arrays.copyOfRange(_params, 1, _params.length);
    String path = PropertyPathConfig.getPath(_type, clusterName, subKeys);
    if(path == null){
      LOG.error("Invalid property key with type:" + _type + "subKeys:" + Arrays.toString(_params));
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
      return new PropertyKey(IDEALSTATES, IdealState.class, _clusterName,
          resourceName);
    }

    public PropertyKey stateModelDefs()
    {
      return new PropertyKey(STATEMODELDEFS, StateModelDefinition.class,
          _clusterName);
    }

    public PropertyKey stateModelDef(String stateModelName)
    {
      return new PropertyKey(STATEMODELDEFS, StateModelDefinition.class,
          _clusterName, stateModelName);
    }

    public PropertyKey clusterConfig()
    {
      return new PropertyKey(CONFIGS, null, _clusterName,
          ConfigScopeProperty.CLUSTER.toString());
    }

    public PropertyKey instanceConfigs()
    {
      return new PropertyKey(CONFIGS, InstanceConfig.class, _clusterName,
          ConfigScopeProperty.PARTICIPANT.toString());
    }

    public PropertyKey instanceConfig(String instanceName)
    {
      return new PropertyKey(CONFIGS, InstanceConfig.class, _clusterName,
          ConfigScopeProperty.PARTICIPANT.toString(), instanceName);
    }

    public PropertyKey resourceConfig(String resourceName)
    {
      return new PropertyKey(CONFIGS, null, _clusterName,
          ConfigScopeProperty.RESOURCE.toString(), resourceName);
    }

    public PropertyKey resourceConfig(String instanceName, String resourceName)
    {
      return new PropertyKey(CONFIGS, null, _clusterName,
          ConfigScopeProperty.RESOURCE.toString(), resourceName);
    }

    public PropertyKey partitionConfig(String resourceName, String partitionName)
    {
      return new PropertyKey(CONFIGS, null, _clusterName,
          ConfigScopeProperty.RESOURCE.toString(), resourceName);
    }

    public PropertyKey partitionConfig(String instanceName,
        String resourceName, String partitionName)
    {
      return new PropertyKey(CONFIGS, null, _clusterName,
          ConfigScopeProperty.RESOURCE.toString(), resourceName);
    }

    public PropertyKey constraints()
    {
      return new PropertyKey(CONFIGS, ClusterConstraints.class, _clusterName,
          ConfigScopeProperty.CONSTRAINT.toString());
    }

    public PropertyKey constraint(String constraintType)
    {
      return new PropertyKey(CONFIGS, ClusterConstraints.class, _clusterName,
          ConfigScopeProperty.CONSTRAINT.toString(), constraintType);
    }

    public PropertyKey liveInstances()
    {
      return new PropertyKey(LIVEINSTANCES, LiveInstance.class, _clusterName);
    }

    public PropertyKey liveInstance(String instanceName)
    {
      return new PropertyKey(LIVEINSTANCES, LiveInstance.class, _clusterName,
          instanceName);
    }

    public PropertyKey instances()
    {
      return new PropertyKey(CONFIGS, null, _clusterName);
    }

    public PropertyKey messages(String instanceName)
    {
      return new PropertyKey(MESSAGES, Message.class, _clusterName,
          instanceName);
    }

    public PropertyKey message(String instanceName, String messageId)
    {
      return new PropertyKey(MESSAGES, Message.class, _clusterName,
          instanceName, messageId);
    }

    public PropertyKey sessions(String instanceName)
    {
      return new PropertyKey(CURRENTSTATES, CurrentState.class, _clusterName,
          instanceName);
    }

    public PropertyKey currentStates(String instanceName, String sessionId)
    {
      return new PropertyKey(CURRENTSTATES, CurrentState.class, _clusterName,
          instanceName, sessionId);
    }

    public PropertyKey currentState(String instanceName, String sessionId,
        String resourceName)
    {
      return new PropertyKey(CURRENTSTATES, CurrentState.class, _clusterName,
          instanceName, sessionId, resourceName);
    }

    public PropertyKey currentState(String instanceName, String sessionId,
        String resourceName, String partitionName)
    {
      return new PropertyKey(CURRENTSTATES, CurrentState.class, _clusterName,
          instanceName, sessionId, resourceName, partitionName);
    }

    // addEntry(PropertyType.STATUSUPDATES, 2,
    // "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES");
    // addEntry(PropertyType.STATUSUPDATES, 3,
    // "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}");
    // addEntry(PropertyType.STATUSUPDATES, 4,
    // "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}");
    // addEntry(PropertyType.STATUSUPDATES, 5,
    // "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}/{recordName}");
    public PropertyKey stateTransitionStatus(String instanceName,
        String sessionId, String resourceName, String partitionName)
    {
      return new PropertyKey(STATUSUPDATES, StatusUpdate.class, _clusterName,
          instanceName, sessionId, resourceName, partitionName);
    }

    public PropertyKey stateTransitionStatus(String instanceName,
        String sessionId, String resourceName)
    {
      return new PropertyKey(STATUSUPDATES, StatusUpdate.class, _clusterName,
          instanceName, sessionId, resourceName);
    }

    public PropertyKey stateTransitionStatus(String instanceName,
        String sessionId)
    {
      return new PropertyKey(STATUSUPDATES, StatusUpdate.class, _clusterName,
          instanceName, sessionId);
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
    public PropertyKey taskStatus(String instanceName, String sessionId,
        String msgType, String msgId)
    {
      return new PropertyKey(STATUSUPDATES, StatusUpdate.class, _clusterName,
          instanceName, sessionId, msgType, msgId);
    }

    public PropertyKey stateTransitionError(String instanceName,
        String sessionId, String resourceName, String partitionName)
    {
      return new PropertyKey(ERRORS, Error.class, _clusterName, instanceName,
          sessionId, resourceName, partitionName);
    }

    public PropertyKey stateTransitionErrors(String instanceName,
        String sessionId, String resourceName)
    {
      return new PropertyKey(ERRORS, Error.class, _clusterName, instanceName,
          sessionId, resourceName);
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
    public PropertyKey taskError(String instanceName, String sessionId,
        String msgType, String msgId)
    {
      return new PropertyKey(ERRORS, null, _clusterName, instanceName,
          sessionId, msgType, msgId);
    }

    public PropertyKey externalViews()
    {
      return new PropertyKey(EXTERNALVIEW, ExternalView.class, _clusterName);
    }

    public PropertyKey externalView(String resourceName)
    {
      return new PropertyKey(EXTERNALVIEW, ExternalView.class, _clusterName,
          resourceName);
    }

    // * addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 4,
    // *
    // "/{clusterName}/CONTROLLER/STATUSUPDATES/{sessionId}/{subPath}/{recordName}"
    // * ); addEntry(PropertyType.LEADER, 1,
    // "/{clusterName}/CONTROLLER/LEADER");
    // * addEntry(PropertyType.HISTORY, 1, "/{clusterName}/CONTROLLER/HISTORY");
    // * addEntry(PropertyType.PAUSE, 1, "/{clusterName}/CONTROLLER/PAUSE");
    // * addEntry(PropertyType.PERSISTENTSTATS, 1,
    // * "/{clusterName}/CONTROLLER/PERSISTENTSTATS");
    // addEntry(PropertyType.ALERTS,
    // * 1, "/{clusterName}/CONTROLLER/ALERTS");
    // addEntry(PropertyType.ALERT_STATUS,
    // * 1, "/{clusterName}/CONTROLLER/ALERT_STATUS");
    // * addEntry(PropertyType.ALERT_HISTORY, 1,
    // * "/{clusterName}/CONTROLLER/ALERT_HISTORY"); // @formatter:on

    public PropertyKey controller()
    {
       return new PropertyKey(CONTROLLER, null,
          _clusterName);
    }
    public PropertyKey controllerTaskErrors()
    {
      return new PropertyKey(ERRORS_CONTROLLER, StatusUpdate.class,
          _clusterName);
    }

    public PropertyKey controllerTaskError(String errorId)
    {
      return new PropertyKey(ERRORS_CONTROLLER, Error.class, _clusterName,
          errorId);
    }

    public PropertyKey controllerTaskStatuses(String subPath)
    {
      return new PropertyKey(STATUSUPDATES_CONTROLLER, StatusUpdate.class,
          _clusterName, subPath);
    }

    public PropertyKey controllerTaskStatus(String subPath, String recordName)
    {
      return new PropertyKey(STATUSUPDATES_CONTROLLER, StatusUpdate.class,
          _clusterName, subPath, recordName);
    }

    public PropertyKey controllerMessages()
    {
      return new PropertyKey(MESSAGES_CONTROLLER, Message.class, _clusterName);
    }

    public PropertyKey controllerMessage(String msgId)
    {
      return new PropertyKey(MESSAGES_CONTROLLER, Message.class, _clusterName,
          msgId);
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
      return new PropertyKey(PERSISTENTSTATS, PersistentStats.class,
          _clusterName);
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
      return new PropertyKey(HEALTHREPORT, HealthStat.class, _clusterName,
          instanceName, id);
    }

    public PropertyKey healthReports(String instanceName)
    {
      return new PropertyKey(HEALTHREPORT, HealthStat.class, _clusterName,
          instanceName);
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

  public static void main(String[] args)
  {
    ZkClient zkClient = new ZkClient("localhost:2181");
    zkClient.waitUntilConnected(10, TimeUnit.SECONDS);
    BaseDataAccessor baseDataAccessor = new ZkBaseDataAccessor(zkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor("test-cluster",
        baseDataAccessor);
    Builder builder = new PropertyKey.Builder("test-cluster");
    HelixProperty value = new IdealState("test-resource");
    accessor.createProperty(builder.idealStates("test-resource"), value);
  }

}
