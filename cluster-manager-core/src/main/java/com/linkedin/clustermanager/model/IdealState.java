package com.linkedin.clustermanager.model;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.controller.stages.BestPossibleStateCalcStage;

public class IdealState
{
  private static final Logger logger = Logger
  .getLogger(IdealState.class.getName());
  private final ZNRecord _record;

  public IdealState()
  {
    _record = new ZNRecord();
  }

  public IdealState(ZNRecord record)
  {
    this._record = record;

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

  public String get(String key, String instanceName)
  {

    Map<String, String> mapField = _record.getMapField(key);
    if (mapField != null)
    {
      return mapField.get(instanceName);
    }
    return null;
  }

  public Set<String> stateUnitSet()
  {
    return _record.getMapFields().keySet();
  }

  public Map<String, String> getInstanceStateMap(String stateUnitKey)
  {
    return _record.getMapField(stateUnitKey);
  }

  public List<String> getInstancePreferenceList(String stateUnitKey)
  {
    LinkedList<String> instanceStateList = new LinkedList<String>();
    Map<String, String> instanceStateMap = getInstanceStateMap(stateUnitKey);
    
    if(instanceStateMap == null)
    {
      logger.info("State unit key "+ stateUnitKey + " not found. It should be dropped from idealstate.");
      return null;
    }

    String masterInstance = "";
    for (String instanceName : instanceStateMap.keySet())
    {
      if (instanceStateMap.get(instanceName).equals("MASTER"))
      {
        masterInstance = instanceName;
      } else
      {
        instanceStateList.add(instanceName);
      }
    }
    assert (!masterInstance.isEmpty());
    instanceStateList.addFirst(masterInstance);

    return instanceStateList;
  }

  public String getStateModelDefRef()
  {
    return _record.getSimpleField("state_model_def_ref");
  }

  public List<String> getPreferenceList(String resourceKeyName)
  {
    if(_record == null)
    {
      return null;
    }
    return getInstancePreferenceList(resourceKeyName);
  }
}
