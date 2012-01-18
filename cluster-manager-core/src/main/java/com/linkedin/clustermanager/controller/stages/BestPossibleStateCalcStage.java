package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.IdealState.IdealStateModeProperty;
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
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    if (currentStateOutput == null || resourceGroupMap == null || cache == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|RESOURCE_GROUPS|DataCache");
    }


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
      logger.debug("Processing resourceGroup:" + resourceGroupName);

      ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
      // Ideal state may be gone. In that case we need to get the state model name
      // from the current state
      IdealState idealState = cache.getIdealState(resourceGroupName);

      String stateModelDefName;

      if (idealState == null)
      {
        // if ideal state is deleted, use an empty one
        logger.info("resourceGroup:" + resourceGroupName + " does not exist anymore");
        stateModelDefName = currentStateOutput.getResourceGroupStateModelDef(resourceGroupName);
        idealState = new IdealState(resourceGroupName);
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
        Set<String> disabledInstancesForResource
          = cache.getDisabledInstancesForResource(resource.toString());

        if (idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED)
        {
          Map<String, String> idealStateMap = idealState.getInstanceStateMap(resource.getResourceKeyName());
          bestStateForResource = computeCustomizedBestStateForResource(cache, stateModelDef,
                                                                       idealStateMap,
                                                                       currentStateMap,
                                                                       disabledInstancesForResource);
        }
        else
        {
          List<String> instancePreferenceList
            = getPreferenceList(cache, resource, idealState, stateModelDef);
          bestStateForResource =
              computeAutoBestStateForResource(cache, stateModelDef,
                                              instancePreferenceList,
                                              currentStateMap,
                                              disabledInstancesForResource);
        }

        output.setState(resourceGroupName, resource, bestStateForResource);
      }
    }
    return output;
  }

  /**
   * compute best state for resource in AUTO ideal state mode
   * @param cache
   * @param stateModelDef
   * @param instancePreferenceList
   * @param currentStateMap
   * @param disabledInstancesForResource
   * @return
   */
  private Map<String, String> computeAutoBestStateForResource(ClusterDataCache cache,
                                                              StateModelDefinition stateModelDef,
                                                              List<String> instancePreferenceList,
                                                              Map<String, String> currentStateMap,
                                                              Set<String> disabledInstancesForResource)
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
        else if (disabledInstancesForResource.contains(instance))
        {
          // if a node is disabled, put it into initial state (OFFLINE)
          // TODO if the node or resource on the node is in ERROR state
          //  shall we reset the current state or let participant provide ERROR->OFFLINE transition?
          instanceStateMap.put(instance, stateModelDef.getInitialState());
        }

      }
    }

    // ideal state is deleted
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
        // stateCount = liveInstancesMap.size();
        Set<String> enabledInstances = liveInstancesMap.keySet();
        enabledInstances.removeAll(disabledInstancesForResource);
        stateCount = enabledInstances.size();
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

          if (liveInstancesMap.containsKey(instanceName) && !assigned[i] && notInErrorState
              && !disabledInstancesForResource.contains(instanceName))
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


  /**
   * compute best state for resource in CUSTOMIZED ideal state mode
   * @param cache
   * @param stateModelDef
   * @param idealStateMap
   * @param currentStateMap
   * @param disabledInstancesForResource
   * @return
   */
  private Map<String, String> computeCustomizedBestStateForResource(ClusterDataCache cache,
                                                                    StateModelDefinition stateModelDef,
                                                                    Map<String, String> idealStateMap,
                                                                    Map<String, String> currentStateMap,
                                                                    Set<String> disabledInstancesForResource)
  {
    Map<String, String> instanceStateMap = new HashMap<String, String>();

    // if the ideal state is deleted, idealStateMap will be null/empty and
    // we should drop all resources.
    if (currentStateMap != null)
    {
      for (String instance : currentStateMap.keySet())
      {
        if (idealStateMap == null || !idealStateMap.containsKey(instance))
        {
          instanceStateMap.put(instance, "DROPPED");
        }
        else if (disabledInstancesForResource.contains(instance))
        {
          // if a node is disabled, put it into initial state (OFFLINE)
          instanceStateMap.put(instance, stateModelDef.getInitialState());
        }
      }
    }

    // ideal state is deleted
    if (idealStateMap == null)
    {
      return instanceStateMap;
    }

    Map<String, LiveInstance> liveInstancesMap = cache.getLiveInstances();
    for (String instance : idealStateMap.keySet())
    {
      boolean notInErrorState =
          currentStateMap == null || !"ERROR".equals(currentStateMap.get(instance));

      if (liveInstancesMap.containsKey(instance) && notInErrorState
          && !disabledInstancesForResource.contains(instance))
      {
        instanceStateMap.put(instance, idealStateMap.get(instance));
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
