package com.linkedin.clustermanager.controller;

import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.model.ZNRecord;
import com.linkedin.clustermanager.util.ZNRecordUtil;

import static com.linkedin.clustermanager.core.CMConstants.ZNAttribute.*;

public class LiveInstanceDataHolder
{
    private Map<String, ZNRecord> _liveInstancesMap;

    public void refresh(List<ZNRecord> liveInstances)
    {
        _liveInstancesMap = ZNRecordUtil.convertListToMap(liveInstances);
    }

    public String getSessionId(String instanceName)
    {
        ZNRecord record = _liveInstancesMap.get(instanceName);
        if (record != null)
        {
            return record.getSimpleField(SESSION_ID.toString());
        }
        return null;
    }

    public boolean isAlive(String instanceName)
    {
        if (_liveInstancesMap != null)
        {
            return _liveInstancesMap.containsKey(instanceName);
        }
        else
        {
            return false;

        }

    }
}
