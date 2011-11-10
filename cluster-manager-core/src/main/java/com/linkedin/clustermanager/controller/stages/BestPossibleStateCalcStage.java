package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor.IdealStateConfigProperty;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

/**
 * For resourceKey compute best possible (instance,state) pair based on
 * IdealState,StateModel,LiveInstance
 *
 * @author kgopalak
 *
 */
public class BestPossibleStateCalcStage extends AbstractBaseStage
{
  private static final Logger logger = Logger.getLogger(BestPossibleStateCalcStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception
  {
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    Map<String, ResourceGroup> resourceGroupMap =
        event.getAttribute(AttributeName.RESOURCE_GROUPS.toString());
    if (currentStateOutput == null || resourceGroupMap == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|RESOURCE_GROUPS");
    }
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    BestPossibleStateOutput bestPossibleStateOutput =
        compute(cache, resourceGroupMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(), bestPossibleStateOutput);
  }

  private BestPossibleStateOutput compute(ClusterDataCache cache,
		Map<String, ResourceGroup> resourceGroupMap,
		CurrentStateOutput currentStateOutput)
  {
    // for each ideal state
    // read the state model def
    // for each resource
    // get the preference list
    // for each instanceName check if its alive then assign a state

    BestPossibleStateOutput output = new BestPossibleStateOutput();

    for (String resourceGroupName : resourceGroupMap.keySet())
    {
      logger.info("Processing resourceGroup:" + resourceGroupName);

      ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
      // Ideal state may be gone. In that case we need to get the state model name
      // from the current state
      IdealState idealState = cache.getIdealState(resourceGroupName);

      String stateModelDefName;

      if (idealState == null)
      {
        // if ideal state is deleted, use an empty ZNRecord
        logger.info(" resourceGroup:" + resourceGroupName + " does not exist anymore");
        stateModelDefName = currentStateOutput.getResourceGroupStateModelDef(resourceGroupName);
        idealState = new IdealState(new ZNRecord(resourceGroupName));
      } else
      {
    	  stateModelDefName = idealState.getStateModelDefRef();
      }

      StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);

      for (ResourceKey resource : resourceGroup.getResourceKeys())
      {
        Map<String, String> currentStateMap =
            currentStateOutput.getCurrentStateMap(resourceGroupName, resource);

        Map<String, String> bestStateForResource;
        if (idealState.getIdealStateMode() == IdealStateConfigProperty.CUSTOMIZED)
        {
          // TODO add computerBestStateForResourceInCustomizedMode()
          bestStateForResource = idealState.getInstanceStateMap(resource.getResourceKeyName());
        }
        else
        {
          List<String> instancePreferenceList =
              getPreferenceList(cache, resource, idealState, stateModelDef);
          bestStateForResource =
              computeBestStateForResource(cache, stateModelDef,
                                          instancePreferenceList,
                                          currentStateMap);
        }

        output.setState(resourceGroupName, resource, bestStateForResource);
      }
    }
    return output;
  }

  private Map<String, String> computeBestStateForResource(ClusterDataCache cache,
  																											  StateModelDefinition stateModelDef,
                                                          List<String> instancePreferenceList,
                                                          Map<String, String> currentStateMap)
  {
    Map<String, String> instanceStateMap = new HashMap<String, String>();

    // if the ideal state is deleted, instancePreferenceList will be empty and
    // we should drop all resources.

    if (currentStateMap != null)
    {
      for (String instance : currentStateMap.keySet())
      {
        if (instancePreferenceList == null || !instancePreferenceList.contains(instance))
        {
          instanceStateMap.put(instance, "DROPPED");
        }
      }
    }

    // use customized ideal states
    if (instancePreferenceList == null)
    {
      return instanceStateMap;
    }

    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    boolean assigned[] = new boolean[instancePreferenceList.size()];

    Map<String, LiveInstance> liveInstancesMap = cache.getLiveInstances();
    for (String state : statesPriorityList)
    {
      String num = stateModelDef.getNumInstancesPerState(state);
      int stateCount = -1;
      if ("N".equals(num))
      {
        stateCount = liveInstancesMap.size();
      }
      else if ("R".equals(num))
      {
        stateCount = instancePreferenceList.size();
      }
      else
      {
        try
        {
          stateCount = Integer.parseInt(num);
        }
        catch (Exception e)
        {
          logger.error("Invalid count for state:" + state + " ,count=" + num);
        }
      }
      if (stateCount > -1)
      {
        int count = 0;
        for (int i = 0; i < instancePreferenceList.size(); i++)
        {
          String instanceName = instancePreferenceList.get(i);

          boolean notInErrorState =
              currentStateMap == null || !"ERROR".equals(currentStateMap.get(instanceName));
          if (liveInstancesMap.containsKey(instanceName) && !assigned[i] && notInErrorState)
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

  private List<String> getPreferenceList(ClusterDataCache cache, ResourceKey resource,
                                         IdealState idealState,
                                         StateModelDefinition stateModelDef)
  {
    List<String> listField =
        idealState.getPreferenceList(resource.getResourceKeyName(), stateModelDef);
    Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    if (listField != null && listField.size() == 1 && "".equals(listField.get(0)))
    {

      ArrayList<String> list = new ArrayList<String>(liveInstances.size());
      for (String instanceName : liveInstances.keySet())
      {
    	  list.add(instanceName);
      }
      return list;

    }
    return listField;
  }

}
