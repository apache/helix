package com.linkedin.clustermanager.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor.IdealStateConfigProperty;
import com.linkedin.clustermanager.ZNRecord;

public class IdealState
{
  public ZNRecord getRecord()
  {
    return _record;
  }

  private static final Logger logger = Logger.getLogger(IdealState.class
      .getName());
  private final ZNRecord _record;
  private final String _resourceGroup;

  public String getResourceGroup()
  {
    return _resourceGroup;
  }

  public IdealState(String resourceGroup)
  {
    _resourceGroup = resourceGroup;
    _record = new ZNRecord(resourceGroup);
  }

  public IdealState(ZNRecord record)
  {
    _resourceGroup = record.getId();
    this._record = record;

  }

  public void setIdealStateMode(String mode)
  {
    _record.setSimpleField("ideal_state_mode", mode);
  }

  public IdealStateConfigProperty getIdealStateMode()
  {
    if (_record == null)
    {
      return IdealStateConfigProperty.AUTO;
    }

    String mode = _record.getSimpleField("ideal_state_mode");
    if (mode == null
        || !mode.equalsIgnoreCase(IdealStateConfigProperty.CUSTOMIZED
            .toString()))
    {
      return IdealStateConfigProperty.AUTO;
    } else
    {
      return IdealStateConfigProperty.CUSTOMIZED;
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
  	if (getIdealStateMode() == IdealStateConfigProperty.AUTO)
  	{
  		return _record.getListFields().keySet();
  	} else if (getIdealStateMode() == IdealStateConfigProperty.CUSTOMIZED)
  	{
  		return _record.getMapFields().keySet();
  	} else
    {
  		logger.warn("Invalid mode in idealstate:" + getResourceGroup());
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
    logger.warn("Resource unit key:" + resourceKeyName
        + " does not have a pre-computed preference list.");
    return null;

  }

  public String getStateModelDefRef()
  {
    return _record.getSimpleField("state_model_def_ref");
  }

  public void setStateModelDefRef(String stateModel)
  {
    _record.setSimpleField("state_model_def_ref", stateModel);
  }

  public List<String> getPreferenceList(String resourceKeyName,
      StateModelDefinition stateModelDef)
  {
    if (_record == null)
    {
      return null;
    }
    return getInstancePreferenceList(resourceKeyName, stateModelDef);
  }

  public void setNumPartitions(int numPartitions)
  {
    _record.setSimpleField("partitions", String.valueOf(numPartitions));
  }

  public int getNumPartitions()
  {
    try
    {
      return Integer.parseInt(_record.getSimpleField("partitions"));
    } catch (Exception e)
    {
      logger.debug("Can't parse number of partitions: " + e);
      return -1;
    }

  }
}
