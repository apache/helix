package com.linkedin.clustermanager.controller;

import java.util.Map;

import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.model.IdealState;

public class BestPossibleIdealStateCalculator
{

    private final LiveInstanceDataHolder _liveInstanceDataHolder;

    public BestPossibleIdealStateCalculator(
            LiveInstanceDataHolder liveInstanceDataHolder)
    {
        this._liveInstanceDataHolder = liveInstanceDataHolder;
    }

    /**
     * @param liveInstances
     * @param idealState
     */
    IdealState compute(IdealState idealState, NotificationContext context)
    {

        IdealState bestPossibleIdealState = new IdealState();
        for (String stateUnitKey : idealState.stateUnitSet())
        {
            Map<String, String> instanceStateMap;
            instanceStateMap = idealState.getInstanceStateMap(stateUnitKey);
            for (String instanceName : instanceStateMap.keySet())
            {
                if (_liveInstanceDataHolder.isAlive(instanceName))
                {
                    bestPossibleIdealState.set(stateUnitKey, instanceName,
                            instanceStateMap.get(instanceName).toString());
                }
            }
        }
        // todo:use live instances
        return bestPossibleIdealState;
    }

}
