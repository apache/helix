/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.controller.stages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixConstants.StateModelToken;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.model.StateModelDefinition;

/**
 * For partition compute best possible (instance,state) pair based on
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
    Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    if (currentStateOutput == null || resourceMap == null || cache == null)
    {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|RESOURCES|DataCache");
    }


    BestPossibleStateOutput bestPossibleStateOutput =
        compute(cache, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(), bestPossibleStateOutput);
  }

  private BestPossibleStateOutput compute(ClusterDataCache cache,
		Map<String, Resource> resourceMap,
		CurrentStateOutput currentStateOutput)
  {
    // for each ideal state
    // read the state model def
    // for each resource
    // get the preference list
    // for each instanceName check if its alive then assign a state

    BestPossibleStateOutput output = new BestPossibleStateOutput();

    for (String resourceName : resourceMap.keySet())
    {
      logger.debug("Processing resource:" + resourceName);

      Resource resource = resourceMap.get(resourceName);
      // Ideal state may be gone. In that case we need to get the state model name
      // from the current state
      IdealState idealState = cache.getIdealState(resourceName);

      String stateModelDefName;

      if (idealState == null)
      {
        // if ideal state is deleted, use an empty one
        logger.info("resource:" + resourceName + " does not exist anymore");
        stateModelDefName = currentStateOutput.getResourceStateModelDef(resourceName);
        idealState = new IdealState(resourceName);
      } else
      {
    	  stateModelDefName = idealState.getStateModelDefRef();
      }

      StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);

      for (Partition partition : resource.getPartitions())
      {
        Map<String, String> currentStateMap =
            currentStateOutput.getCurrentStateMap(resourceName, partition);

        Map<String, String> bestStateForPartition;
        Set<String> disabledInstancesForPartition
          = cache.getDisabledInstancesForPartition(partition.toString());

        if (idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED)
        {
          Map<String, String> idealStateMap = idealState.getInstanceStateMap(partition.getPartitionName());
          bestStateForPartition = computeCustomizedBestStateForPartition(cache, stateModelDef,
                                                                       idealStateMap,
                                                                       currentStateMap,
                                                                       disabledInstancesForPartition);
        }
        else
        {
          List<String> instancePreferenceList
            = getPreferenceList(cache, partition, idealState, stateModelDef);
          bestStateForPartition =
              computeAutoBestStateForPartition(cache, stateModelDef,
                                              instancePreferenceList,
                                              currentStateMap,
                                              disabledInstancesForPartition);
        }

        output.setState(resourceName, partition, bestStateForPartition);
      }
    }
    return output;
  }

  /**
   * compute best state for resource in AUTO ideal state mode
   *
   * @param cache
   * @param stateModelDef
   * @param instancePreferenceList
   * @param currentStateMap
   *          : instance->state for each partition
   * @param disabledInstancesForPartition
   * @return
   */
  private Map<String, String> computeAutoBestStateForPartition(ClusterDataCache cache,
                                                              StateModelDefinition stateModelDef,
                                                              List<String> instancePreferenceList,
                                                              Map<String, String> currentStateMap,
                                                              Set<String> disabledInstancesForPartition)
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
        else if (!"ERROR".equals(currentStateMap.get(instance))
            && disabledInstancesForPartition.contains(instance))
        {
          // if a non-error node is disabled, put it into initial state (OFFLINE)
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
        Set<String> liveAndEnabled = new HashSet<String>(liveInstancesMap.keySet());
        liveAndEnabled.removeAll(disabledInstancesForPartition);
        stateCount = liveAndEnabled.size();
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
              && !disabledInstancesForPartition.contains(instanceName))
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
   * @param disabledInstancesForPartition
   * @return
   */
  private Map<String, String> computeCustomizedBestStateForPartition(ClusterDataCache cache,
                                                                    StateModelDefinition stateModelDef,
                                                                    Map<String, String> idealStateMap,
                                                                    Map<String, String> currentStateMap,
                                                                    Set<String> disabledInstancesForPartition)
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
        else if (!"ERROR".equals(currentStateMap.get(instance))
            && disabledInstancesForPartition.contains(instance))
        {
          // if a non-error node is disabled, put it into initial state (OFFLINE)
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
          && !disabledInstancesForPartition.contains(instance))
      {
        instanceStateMap.put(instance, idealStateMap.get(instance));
      }
    }

    return instanceStateMap;
  }


  private List<String> getPreferenceList(ClusterDataCache cache, Partition resource,
                                         IdealState idealState,
                                         StateModelDefinition stateModelDef)
  {
    List<String> listField =
        idealState.getPreferenceList(resource.getPartitionName(), stateModelDef);

    if (listField != null && listField.size() == 1
        && StateModelToken.ANY_LIVEINSTANCE.toString().equals(listField.get(0)))
    {
      Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
      List<String> prefList = new ArrayList<String>(liveInstances.keySet());
      Collections.sort(prefList);
      return prefList;
    }
    else
    {
      return listField;
    }
  }
}
