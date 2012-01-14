package com.linkedin.clustermanager.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordDecorator;

/**
 * The ideal states of all resources in a resource group
 */
public class IdealState extends ZNRecordDecorator
{
  public enum IdealStateProperty
  {
    RESOURCES,
    STATE_MODEL_DEF_REF,
    REPLICAS,
    IDEAL_STATE_MODE
  }

  public enum IdealStateModeProperty
  {
    AUTO,
    CUSTOMIZED
  }

  private static final Logger logger = Logger.getLogger(IdealState.class.getName());

  public IdealState(String resourceGroup)
  {
    super(resourceGroup);
  }

  public IdealState(ZNRecord record)
  {
    super(record);
  }

  public String getResourceGroup()
  {
    return _record.getId();
  }

  public void setIdealStateMode(String mode)
  {
    _record.setSimpleField(IdealStateProperty.IDEAL_STATE_MODE.toString(), mode);
  }

  public IdealStateModeProperty getIdealStateMode()
  {
    String mode = _record.getSimpleField(IdealStateProperty.IDEAL_STATE_MODE.toString());
    if (mode == null
      || !mode.equalsIgnoreCase(IdealStateModeProperty.CUSTOMIZED.toString()))
    {
      return IdealStateModeProperty.AUTO;
    }
    else
    {
      return IdealStateModeProperty.CUSTOMIZED;
    }
  }

  public void set(String key, String instanceName, String state)
  {
    Map<String, String> mapField = _record.getMapField(key);
    if (mapField == null)
    {
      _record.setMapField(key, new TreeMap<String, String>());
    }
    _record.getMapField(key).put(instanceName, state);
  }

  public Set<String> getResourceKeySet()
  {
    if (getIdealStateMode() == IdealStateModeProperty.AUTO)
    {
      return _record.getListFields().keySet();
    }
    else if (getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED)
    {
      return _record.getMapFields().keySet();
    }
    else
    {
      logger.error("Invalid ideal state mode:" + getResourceGroup());
      return Collections.emptySet();
    }
  }

  public Map<String, String> getInstanceStateMap(String resourceKeyName)
  {
    return _record.getMapField(resourceKeyName);
  }

  private List<String> getInstancePreferenceList(String resourceKeyName,
                                                 StateModelDefinition stateModelDef)
  {
    List<String> instanceStateList = _record.getListField(resourceKeyName);

    if (instanceStateList != null)
    {
      return instanceStateList;
    }
    logger.warn("Resource key:" + resourceKeyName
        + " does not have a pre-computed preference list.");
    return null;
  }

  public String getStateModelDefRef()
  {
    return _record.getSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString());
  }

  public void setStateModelDefRef(String stateModel)
  {
    _record.setSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString(), stateModel);
  }

  public List<String> getPreferenceList(String resourceKeyName,
                                        StateModelDefinition stateModelDef)
  {
    return getInstancePreferenceList(resourceKeyName, stateModelDef);
  }

  public void setNumPartitions(int numPartitions)
  {
    _record.setSimpleField(IdealStateProperty.RESOURCES.toString(), String.valueOf(numPartitions));
  }

  public int getNumPartitions()
  {
    try
    {
      return Integer.parseInt(_record.getSimpleField(IdealStateProperty.RESOURCES.toString()));
    }
    catch (Exception e)
    {
      logger.debug("Can't parse number of partitions: " + e);
      return -1;
    }
  }

  public void setReplicas(int replicas)
  {
    _record.setSimpleField(IdealStateProperty.REPLICAS.toString(), Integer.toString(replicas));
  }
  
  public int getReplicas()
  {
    try
    {
      return Integer.parseInt(_record.getSimpleField(IdealStateProperty.REPLICAS.toString()));
    }
    catch(Exception e)
    {}
    return -1;
  }

  @Override
  public boolean isValid()
  {
    if(getNumPartitions() < 0)
    {
      logger.error("idealStates does not have number of partitions. IS:" + _record.getId());
      return false;
    }
    if(getReplicas() < 0)
    {
      logger.error("idealStates does not have replicas. IS:" + _record.getId());
      return false;
    }
    if(getStateModelDefRef() == null)
    {
      logger.error("idealStates does not have state model def. IS:" + _record.getId());
      return false;
    }
    return true;
  }
}
