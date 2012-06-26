package com.linkedin.helix;

import static com.linkedin.helix.PropertyType.*;

import java.util.concurrent.TimeUnit;

import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.IdealState;

public class PropertyKey
{

  public PropertyType _type;
  private String[] _params;
  public PropertyKey(PropertyType type, String... params)
  {
    _type = type;
    _params = params;
  }
  
  @Override
  public int hashCode()
  {
    return super.hashCode();
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
      return new PropertyKey(IDEALSTATES, _clusterName);
    }

    public PropertyKey idealStates(String resourceName)
    {
      return new PropertyKey(IDEALSTATES, _clusterName, resourceName);
    }

    public PropertyKey stateModelDefs()
    {
      return new PropertyKey(STATEMODELDEFS, _clusterName);
    }

    public PropertyKey stateModelDef(String stateModelName)
    {
      return new PropertyKey(STATEMODELDEFS, _clusterName, stateModelName);
    }

    public PropertyKey clusterConfig()
    {
      return new PropertyKey(CONFIGS, _clusterName);
    }

    public PropertyKey instanceConfigs()
    {
      return new PropertyKey(CONFIGS, _clusterName,
          ConfigScopeProperty.PARTICIPANT.toString());
    }

    public PropertyKey instanceConfig(String instanceName)
    {
      return new PropertyKey(CONFIGS, _clusterName, instanceName);
    }

    public PropertyKey resourceConfig(String resourceName)
    {
      return new PropertyKey(CONFIGS, _clusterName, resourceName);
    }

    public PropertyKey resourceConfig(String instanceName, String resourceName)
    {
      return new PropertyKey(CONFIGS, _clusterName, resourceName);
    }

    public PropertyKey partitionConfig(String resourceName, String partitionName)
    {
      return new PropertyKey(CONFIGS, _clusterName, resourceName);
    }

    public PropertyKey partitionConfig(String instanceName,
        String resourceName, String partitionName)
    {
      return new PropertyKey(CONFIGS, _clusterName, resourceName);
    }

    public PropertyKey constraints()
    {
      return new PropertyKey(CONFIGS, _clusterName,
          ConfigScopeProperty.CONSTRAINT.toString());
    }

    public PropertyKey liveInstances()
    {
      return new PropertyKey(LIVEINSTANCES, _clusterName);
    }

    public PropertyKey liveInstance(String instanceName)
    {
      return new PropertyKey(LIVEINSTANCES, _clusterName, instanceName);
    }

    public PropertyKey instances()
    {
      return new PropertyKey(CONFIGS, _clusterName);
    }

    public PropertyKey messages(String instanceName)
    {
      return new PropertyKey(CONFIGS, _clusterName, instanceName);
    }

    public PropertyKey message(String instanceName, String messageId)
    {
      return new PropertyKey(CONFIGS, _clusterName, instanceName, messageId);
    }

    public PropertyKey sessions(String instanceName)
    {
      return new PropertyKey(CURRENTSTATES, _clusterName, instanceName);
    }

    public PropertyKey currentStates(String instanceName, String sessionId)
    {
      return new PropertyKey(CONFIGS, _clusterName, instanceName, sessionId);
    }

    public PropertyKey currentState(String instanceName, String sessionId,
        String resourceName)
    {
      return new PropertyKey(CONFIGS, _clusterName, instanceName, sessionId,
          resourceName);
    }

    public PropertyKey currentState(String instanceName, String sessionId,
        String resourceName, String partitionName)
    {
      return new PropertyKey(CONFIGS, _clusterName, instanceName, sessionId,
          resourceName, partitionName);
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
      return new PropertyKey(STATUSUPDATES, _clusterName, instanceName,
          sessionId, partitionName);
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
      return new PropertyKey(STATUSUPDATES, _clusterName, instanceName,
          sessionId, msgType, msgId);
    }

    public PropertyKey stateTransitionError(String instanceName,
        String sessionId, String resourceName, String partitionName)
    {
      return new PropertyKey(ERRORS, _clusterName, instanceName, sessionId,
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
    public PropertyKey taskError(String instanceName, String sessionId,
        String msgType, String msgId)
    {
      return new PropertyKey(ERRORS, _clusterName, instanceName, sessionId,
          msgType, msgId);
    }

    public PropertyKey externalViews()
    {
      return new PropertyKey(EXTERNALVIEW, _clusterName);
    }

    public PropertyKey externalView(String resourceName)
    {
      return new PropertyKey(EXTERNALVIEW, _clusterName, resourceName);
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
        String sessionId, String msgType, String msgId)
    {
      return new PropertyKey(ERRORS_CONTROLLER, _clusterName, instanceName,
          sessionId, msgType, msgId);
    }

    public PropertyKey controllerTaskStatus(String instanceName,
        String sessionId, String msgType, String msgId)
    {
      return new PropertyKey(STATUSUPDATES_CONTROLLER, _clusterName,
          instanceName, sessionId, msgType, msgId);
    }

    public PropertyKey controllerLeaderHistory()
    {
      return new PropertyKey(HISTORY, _clusterName);
    }

    public PropertyKey controllerLeader()
    {
      return new PropertyKey(LEADER, _clusterName);
    }

    public PropertyKey pause()
    {
      return new PropertyKey(PAUSE, _clusterName);
    }

    public PropertyKey persistantStat()
    {
      return new PropertyKey(PERSISTENTSTATS, _clusterName);
    }

    public PropertyKey alerts()
    {
      return new PropertyKey(ALERTS, _clusterName);
    }

    public PropertyKey alertStatus()
    {
      return new PropertyKey(ALERT_STATUS, _clusterName);
    }

    public PropertyKey alertHistory()
    {
      return new PropertyKey(ALERT_HISTORY, _clusterName);
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
    return null;
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
