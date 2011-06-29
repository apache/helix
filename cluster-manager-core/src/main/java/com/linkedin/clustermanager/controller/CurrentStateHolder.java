package com.linkedin.clustermanager.controller;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.core.CMConstants.ZNAttribute;
import com.linkedin.clustermanager.model.ZNRecord;

public class CurrentStateHolder
{
    private LinkedHashMap<String, Map<String, String>> _currentStatesMap;
    // currently zkrouting info needs this format,
    // TODO: merge th currentStatesMap and currentStatesListMap later
    private Map<String, List<ZNRecord>> _currentStatesListMap;

    public CurrentStateHolder()
    {
        _currentStatesMap = new LinkedHashMap<String, Map<String, String>>();
        _currentStatesListMap = new HashMap<String, List<ZNRecord>>();
    }

    public LinkedHashMap<String, Map<String, String>> getCurrentStatesMap()
    {
        return _currentStatesMap;
    }

    public String getState(String stateUnitKey, String instanceName)
    {
        Map<String, String> map = _currentStatesMap.get(instanceName);
        if (map != null)
        {
            return map.get(stateUnitKey);
        }
        return null;
    }

    public void refresh(String instanceName, List<ZNRecord> currentStates)
    {
        Map<String, String> map = new HashMap<String, String>();
        for (ZNRecord record : currentStates)
        {
            Map<String, Map<String, String>> mapFields = record.getMapFields();
            for (String stateUnitKey : mapFields.keySet())
            {
                String currentStateKey;
                currentStateKey = ZNAttribute.CURRENT_STATE.toString();
                map.put(stateUnitKey,
                        mapFields.get(stateUnitKey).get(currentStateKey));
            }
        }
        _currentStatesMap.put(instanceName, map);
        _currentStatesListMap.put(instanceName, currentStates);
    }

    public Map<String, List<ZNRecord>> getCurrentStatesListMap()
    {
        return _currentStatesListMap;
    }

	public void clear() {
		_currentStatesMap.clear();
		_currentStatesListMap.clear();
	}

}
