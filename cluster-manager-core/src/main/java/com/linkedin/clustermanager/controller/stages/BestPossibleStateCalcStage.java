package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;
import com.linkedin.clustermanager.util.ZNRecordUtil;

public class BestPossibleStateCalcStage extends AbstractBaseStage
{

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    ClusterManager manager = event.getAttribute("clustermanager");
    if (manager == null)
    {
      throw new StageException("ClusterManager attribute value is null");
    }
    ClusterDataAccessor dataAccessor = manager.getDataAccessor();
    List<ZNRecord> liveInstances;
    liveInstances = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.LIVEINSTANCES);
    List<ZNRecord> idealStates = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.IDEALSTATES);
    List<ZNRecord> stateModelDefs = dataAccessor
        .getClusterPropertyList(ClusterPropertyType.STATEMODELDEFS);
    CurrentStateOutput currentStateOutput = event
        .getAttribute(AttributeName.CURRENT_STATE.toString());
    Map<String, ResourceGroup> resourceGroupMap = event
        .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    BestPossibleStateOutput bestPossibleStateOutput = compute(resourceGroupMap,
        liveInstances, idealStates, stateModelDefs, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(),
        bestPossibleStateOutput);
  }

  private BestPossibleStateOutput compute(
      Map<String, ResourceGroup> resourceGroupMap,
      List<ZNRecord> liveInstances, List<ZNRecord> idealStates,
      List<ZNRecord> stateModelDefs, CurrentStateOutput currentStateOutput)
  {
    // for each ideal state
    // read the state model def
    // for each resource
    // get the preference list
    // for each instanceName check if its alive then assign a state
    Map<String, ZNRecord> liveInstancesMap = ZNRecordUtil
        .convertListToMap(liveInstances);

    Map<String, ZNRecord> idealStatesMap = ZNRecordUtil
        .convertListToMap(idealStates);
    BestPossibleStateOutput output = new BestPossibleStateOutput();

    for (String resourceGroupName : resourceGroupMap.keySet())
    {
      ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
      StateModelDefinition stateModelDef = lookupStateModel(resourceGroupName,
          stateModelDefs);
      for (ResourceKey resource : resourceGroup.getResourceKeys())
      {
        ZNRecord idealState = idealStatesMap.get(resourceGroupName);
        List<String> instancePreferenceList = getPreferenceList(resource,
            idealState, liveInstances);

        Map<String, String> currentStateMap = currentStateOutput
            .getCurrentStateMap(resourceGroupName, resource);
        Map<String, String> bestStateForResource = computeBestStateForResource(
            stateModelDef, instancePreferenceList, liveInstancesMap,
            currentStateMap);
        output.setState(resourceGroupName, resource, bestStateForResource);
      }
    }
    return output;
  }

  private Map<String, String> computeBestStateForResource(
      StateModelDefinition stateModelDef, List<String> instancePreferenceList,
      Map<String, ZNRecord> liveInstancesMap,
      Map<String, String> currentStateMap)
  {
    Map<String, String> instanceStateMap = new HashMap<String, String>();
    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    boolean assigned[] = new boolean[instancePreferenceList.size()];

    for (String state : statesPriorityList)
    {
      String num = stateModelDef.getNumInstancesPerState(state);
      int stateCount = -1;
      if ("N".equals(num))
      {
        stateCount = liveInstancesMap.size();
      } else if ("R".equals(num))
      {
        stateCount = instancePreferenceList.size();
      } else
      {
        try
        {
          stateCount = Integer.parseInt(num);
        } catch (Exception e)
        {

        }
      }
      if (stateCount > -1)
      {
        int count = 0;
        for (int i = 0; i < instancePreferenceList.size(); i++)
        {
          String instanceName = instancePreferenceList.get(i);

          if (liveInstancesMap.containsKey(instanceName) && !assigned[i]
              && !"ERROR".equals(currentStateMap.get(instanceName)))
          {
            instanceStateMap.put(instanceName, state);
            count = count + 1;
            assigned[i] = true;
            if (count == stateCount)
            {
              break;
            }
          }
        }
      }
    }
    return instanceStateMap;
  }

  private List<String> getPreferenceList(ResourceKey resource,
      ZNRecord idealState, List<ZNRecord> liveInstances)
  {
    List<String> listField = idealState.getListField(resource
        .getResourceKeyName());
    if (listField.size() == 1 && "".equals(listField.get(0)))
    {
      ArrayList<String> list = new ArrayList<String>(liveInstances.size());
      for (ZNRecord liveInstance : liveInstances)
      {
        list.add(liveInstance.getId());
      }
      return list;
    }
    return listField;
  }

  private StateModelDefinition lookupStateModel(String stateModelDefRef,
      List<ZNRecord> stateModelDefs)
  {
    for (ZNRecord record : stateModelDefs)
    {
      if (record.getId().equals(stateModelDefRef))
      {
        return new StateModelDefinition(record);
      }
    }
    return null;
  }
}
