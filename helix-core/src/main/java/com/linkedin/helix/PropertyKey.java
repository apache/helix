package com.linkedin.helix;

import static com.linkedin.helix.PropertyType.ALERTS;
import static com.linkedin.helix.PropertyType.ALERT_HISTORY;
import static com.linkedin.helix.PropertyType.ALERT_STATUS;
import static com.linkedin.helix.PropertyType.CONFIGS;
import static com.linkedin.helix.PropertyType.CURRENTSTATES;
import static com.linkedin.helix.PropertyType.ERRORS;
import static com.linkedin.helix.PropertyType.ERRORS_CONTROLLER;
import static com.linkedin.helix.PropertyType.EXTERNALVIEW;
import static com.linkedin.helix.PropertyType.HISTORY;
import static com.linkedin.helix.PropertyType.IDEALSTATES;
import static com.linkedin.helix.PropertyType.LEADER;
import static com.linkedin.helix.PropertyType.LIVEINSTANCES;
import static com.linkedin.helix.PropertyType.MESSAGES;
import static com.linkedin.helix.PropertyType.PAUSE;
import static com.linkedin.helix.PropertyType.PERSISTENTSTATS;
import static com.linkedin.helix.PropertyType.STATEMODELDEFS;
import static com.linkedin.helix.PropertyType.STATUSUPDATES;
import static com.linkedin.helix.PropertyType.STATUSUPDATES_CONTROLLER;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ClusterConstraints;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.StateModelDefinition;

public class PropertyKey
{

  public PropertyType    _type;
  private final String[] _params;
  Class<? extends HelixProperty> _typeClazz;

  public PropertyKey(PropertyType type, Class<? extends HelixProperty> typeClazz, String... params)
  {
    _type = type;
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
    return PropertyPathConfig.getPath(_type, clusterName, subKeys);
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
      return new PropertyKey(STATEMODELDEFS, StateModelDefinition.class, _clusterName, stateModelName);
    }

    public PropertyKey clusterConfig()
    {
      return new PropertyKey(CONFIGS, null, _clusterName);
    }

    public PropertyKey instanceConfigs()
    {
      return new PropertyKey(CONFIGS,
                             InstanceConfig.class,
                             _clusterName,
                             ConfigScopeProperty.PARTICIPANT.toString());
    }

    public PropertyKey instanceConfig(String instanceName)
    {
      return new PropertyKey(CONFIGS, InstanceConfig.class, _clusterName, instanceName);
    }

    public PropertyKey resourceConfig(String resourceName)
    {
      return new PropertyKey(CONFIGS, null, _clusterName, resourceName);
    }

    public PropertyKey resourceConfig(String instanceName, String resourceName)
    {
      return new PropertyKey(CONFIGS, null, _clusterName, resourceName);
    }

    public PropertyKey partitionConfig(String resourceName, String partitionName)
    {
      return new PropertyKey(CONFIGS, null, _clusterName, resourceName);
    }

    public PropertyKey partitionConfig(String instanceName,
                                       String resourceName,
                                       String partitionName)
    {
      return new PropertyKey(CONFIGS, null, _clusterName, resourceName);
    }

    public PropertyKey constraints()
    {
      return new PropertyKey(CONFIGS,
                             ClusterConstraints.class,
                             _clusterName,
                             ConfigScopeProperty.CONSTRAINT.toString());
    }

    public PropertyKey liveInstances()
    {
      return new PropertyKey(LIVEINSTANCES, LiveInstance.class, _clusterName);
    }

    public PropertyKey liveInstance(String instanceName)
    {
      return new PropertyKey(LIVEINSTANCES, LiveInstance.class, _clusterName, instanceName);
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
      return new PropertyKey(MESSAGES, Message.class, _clusterName, instanceName, messageId);
    }

    public PropertyKey sessions(String instanceName)
    {
      return new PropertyKey(CURRENTSTATES, CurrentState.class, _clusterName, instanceName);
    }

    public PropertyKey currentStates(String instanceName, String sessionId)
    {
      return new PropertyKey(CURRENTSTATES, CurrentState.class, _clusterName, instanceName, sessionId);
    }

    public PropertyKey currentState(String instanceName,
                                    String sessionId,
                                    String resourceName)
    {
      return new PropertyKey(CURRENTSTATES, CurrentState.class, _clusterName, instanceName, sessionId, resourceName);
    }

    public PropertyKey currentState(String instanceName,
                                    String sessionId,
                                    String resourceName,
                                    String partitionName)
    {
      return new PropertyKey(CURRENTSTATES,
                             CurrentState.class,
                             _clusterName,
                             instanceName,
                             sessionId,
                             resourceName,
                             partitionName);
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
                                             String sessionId,
                                             String resourceName,
                                             String partitionName)
    {
      return new PropertyKey(STATUSUPDATES,
                             null,
                             _clusterName,
                             instanceName,
                             sessionId,
                             partitionName);
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
                             null,
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
      return new PropertyKey(ERRORS, null, _clusterName, instanceName, sessionId, partitionName);
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

    public PropertyKey controllerTaskError(String instanceName,
                                           String sessionId,
                                           String msgType,
                                           String msgId)
    {
      return new PropertyKey(ERRORS_CONTROLLER,
                             null,
                             _clusterName,
                             instanceName,
                             sessionId,
                             msgType,
                             msgId);
    }

    public PropertyKey controllerTaskStatus(String instanceName,
                                            String sessionId,
                                            String msgType,
                                            String msgId)
    {
      return new PropertyKey(STATUSUPDATES_CONTROLLER,
                             null,
                             _clusterName,
                             instanceName,
                             sessionId,
                             msgType,
                             msgId);
    }

    public PropertyKey controllerLeaderHistory()
    {
      return new PropertyKey(HISTORY, null, _clusterName);
    }

    public PropertyKey controllerLeader()
    {
      return new PropertyKey(LEADER, LiveInstance.class, _clusterName);
    }

    public PropertyKey pause()
    {
      return new PropertyKey(PAUSE, null, _clusterName);
    }

    public PropertyKey persistantStat()
    {
      return new PropertyKey(PERSISTENTSTATS, null, _clusterName);
    }

    public PropertyKey alerts()
    {
      return new PropertyKey(ALERTS, null, _clusterName);
    }

    public PropertyKey alertStatus()
    {
      return new PropertyKey(ALERT_STATUS, null, _clusterName);
    }

    public PropertyKey alertHistory()
    {
      return new PropertyKey(ALERT_HISTORY, null, _clusterName);
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
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor("test-cluster", baseDataAccessor);
    Builder builder = new PropertyKey.Builder("test-cluster");
    HelixProperty value = new IdealState("test-resource");
    accessor.createProperty(builder.idealStates("test-resource"), value);
  }

}
