package com.linkedin.helix.controller.stages;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.HelixConstants.StateModelToken;
import com.linkedin.helix.controller.pipeline.AbstractBaseStage;
import com.linkedin.helix.controller.pipeline.StageException;
import com.linkedin.helix.josql.JsqlQueryListProcessor;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.model.Resource;
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
    long startTime = System.currentTimeMillis();
    logger.info("START BestPossibleStateCalcStage.process()");

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
        compute(event, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(), bestPossibleStateOutput);
    
    long endTime = System.currentTimeMillis();
    logger.info("END BestPossibleStateCalcStage.process(). took: " + (endTime - startTime) + " ms");
  }

  private BestPossibleStateOutput compute(ClusterEvent event,
		Map<String, Resource> resourceMap,
		CurrentStateOutput currentStateOutput)
  {
    // for each ideal state
    // read the state model def
    // for each resource
    // get the preference list
    // for each instanceName check if its alive then assign a state
    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    HelixManager manager = event.getAttribute("helixmanager");
    
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
      if(idealState.getIdealStateMode() == IdealStateModeProperty.AUTO_REBALANCE)
      {
        calculateAutoBalancedIdealState(cache, idealState, stateModelDef, currentStateOutput);
      }
      
      // For idealstate that has rebalancing timer and is in AUTO mode, we will run jsql queries to calculate the 
      // preference list
      
      Map<String, List<String>> queryPartitionPriorityLists = null;
      if(idealState.getIdealStateMode() == IdealStateModeProperty.AUTO && idealState.getRebalanceTimerPeriod() > 0)
      {
        if(manager != null)
        {
          queryPartitionPriorityLists = calculatePartitionPriorityListWithQuery(manager, idealState);
        }
      }

      for (Partition partition : resource.getPartitions())
      {
        Map<String, String> currentStateMap =
            currentStateOutput.getCurrentStateMap(resourceName, partition);

        Map<String, String> bestStateForPartition;
        Set<String> disabledInstancesForPartition
          = cache.getDisabledInstancesForPartition(partition.toString());

        if (idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED )
        {
          Map<String, String> idealStateMap = idealState.getInstanceStateMap(partition.getPartitionName());
          bestStateForPartition = computeCustomizedBestStateForPartition(cache, stateModelDef,
                                                                       idealStateMap,
                                                                       currentStateMap,
                                                                       disabledInstancesForPartition);
        }
        else // both AUTO and AUTO_REBALANCE mode
        {
          List<String> instancePreferenceList
            = getPreferenceList(cache, partition, idealState, stateModelDef);
          if(queryPartitionPriorityLists != null)
          {
            String partitionName = partition.getPartitionName();
            if(queryPartitionPriorityLists.containsKey(partitionName))
            {
              List<String> queryInstancePreferenceList = queryPartitionPriorityLists.get(partitionName);
              // For instances that is not included in the queryInstancePreferenceList, add them to the end of the list
              for(String instanceName : instancePreferenceList)
              {
                if(!queryInstancePreferenceList.contains(instanceName))
                {
                  queryInstancePreferenceList.add(instanceName);
                }
              }
              instancePreferenceList = queryInstancePreferenceList;
            }
          }
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
  
  Map<String, List<String>> calculatePartitionPriorityListWithQuery(HelixManager manager, IdealState idealState)
  {
    // Read queries from the resource config
    String querys = idealState.getRecord().getSimpleField(IdealState.QUERY_LIST);
    if(querys == null)
    {
      logger.warn("IdealState " + idealState.getResourceName() + " does not have query list");
      return null;
    }
    try
    {
      List<String> queryList = Arrays.asList(querys.split(";"));
      List<ZNRecord> resultList = JsqlQueryListProcessor.executeQueryList(manager.getHelixDataAccessor(),manager.getClusterName(), queryList);
      Map<String, List<String>> priorityLists = new TreeMap<String, List<String>>();
      for(ZNRecord result : resultList)
      {
        String partition = result.getSimpleField("partition");
        String instance = result.getSimpleField("instance");
        if(instance.equals("") || partition.equals(""))
        {
          continue;
        }
        
        if(!priorityLists.containsKey(partition))
        {
          priorityLists.put(partition, new ArrayList<String>());
        }
        priorityLists.get(partition).add(instance);
      }
      return priorityLists;
    }
    catch (Exception e)
    {
      logger.error("", e);
      return null;
    }
    
  }

  /**
   * Compute best state for resource in AUTO_REBALANCE ideal state mode.
   * the algorithm will make sure that the master partition are evenly distributed;
   * Also when instances are added / removed, the amount of diff in master partitions
   * are minimized
   *
   * @param cache
   * @param idealState
   * @param instancePreferenceList
   * @param stateModelDef
   * @param currentStateOutput
   * @return
   */
  private void calculateAutoBalancedIdealState(ClusterDataCache cache, 
      IdealState idealState, 
      StateModelDefinition stateModelDef,
      CurrentStateOutput currentStateOutput)
  {
    String topStateValue = stateModelDef.getStatesPriorityList().get(0);
    Set<String> liveInstances = cache._liveInstanceMap.keySet();
 // Obtain replica number
    int replicas = 1;
    try
    {
      replicas = Integer.parseInt(idealState.getReplicas());
    }
    catch(Exception e)
    {
      logger.error("",e);
    }
    
    Map<String, List<String>> masterAssignmentMap = new HashMap<String, List<String>>();
    for(String instanceName : liveInstances)
    {
      masterAssignmentMap.put(instanceName, new ArrayList<String>());
    }
    Set<String> orphanedPartitions  = new HashSet<String>();
    orphanedPartitions.addAll(idealState.getPartitionSet());
    // Go through all current states and fill the assignments
    for(String liveInstanceName : liveInstances)
    {
      CurrentState currentState 
        = cache.getCurrentState(liveInstanceName, cache.getLiveInstances().get(liveInstanceName).getSessionId()).get(idealState.getId());
      if(currentState != null)
      {
        Map<String, String> partitionStates = currentState.getPartitionStateMap();
        for(String partitionName : partitionStates.keySet())
        {
          String state = partitionStates.get(partitionName);
          if(state.equals(topStateValue))
          {
            masterAssignmentMap.get(liveInstanceName).add(partitionName);
            orphanedPartitions.remove(partitionName);
          }
        }
      }
    }
    List<String> orphanedPartitionsList = new ArrayList<String>();
    orphanedPartitionsList.addAll(orphanedPartitions);
    normalizeAssignmentMap(masterAssignmentMap, orphanedPartitionsList);
    idealState.getRecord().setListFields(generateListFieldFromMasterAssignment(masterAssignmentMap, replicas));
    
  }
  /**
   * Given the current master assignment map and the partitions not hosted,
   * generate an evenly distributed partition assignment map
   *
   * @param masterAssignmentMap current master assignment map
   * @param orphanPartitions partitions not hosted by any instance
   * @return
   */
  private void normalizeAssignmentMap(
      Map<String, List<String>> masterAssignmentMap,
      List<String> orphanPartitions)
  {
    int totalPartitions = 0;
    String[] instanceNames = new String[masterAssignmentMap.size()];
    masterAssignmentMap.keySet().toArray(instanceNames);
    Arrays.sort(instanceNames);
    // Find out total partition number
    for(String key : masterAssignmentMap.keySet())
    {
      totalPartitions += masterAssignmentMap.get(key).size();
      Collections.sort(masterAssignmentMap.get(key));
    }
    totalPartitions += orphanPartitions.size();
    
    // Find out how many partitions an instance should host
    int partitionNumber = totalPartitions / masterAssignmentMap.size();
    int leave = totalPartitions % masterAssignmentMap.size();
    
    for(int i = 0; i < instanceNames.length; i++)
    {
      int targetPartitionNo = leave > 0 ? (partitionNumber + 1) : partitionNumber;
      leave --;
      // For hosts that has more partitions, move those partitions to "orphaned"
      while(masterAssignmentMap.get(instanceNames[i]).size() > targetPartitionNo)
      {
        int lastElementIndex = masterAssignmentMap.get(instanceNames[i]).size() - 1;
        orphanPartitions.add(masterAssignmentMap.get(instanceNames[i]).get(lastElementIndex));
        masterAssignmentMap.get(instanceNames[i]).remove(lastElementIndex);
      }
    }
    leave = totalPartitions % masterAssignmentMap.size();
    Collections.sort(orphanPartitions);
    // Assign "orphaned" partitions to hosts that do not have enough partitions
    for(int i = 0; i <  instanceNames.length; i++)
    {
      int targetPartitionNo = leave > 0 ? (partitionNumber + 1) : partitionNumber;
      leave --;
      while(masterAssignmentMap.get(instanceNames[i]).size() < targetPartitionNo)
      {
        int lastElementIndex = orphanPartitions.size() - 1;
        masterAssignmentMap.get(instanceNames[i]).add(orphanPartitions.get(lastElementIndex));
        orphanPartitions.remove(lastElementIndex);
      }
    }
    if(orphanPartitions.size() > 0)
    {
      logger.error("orphanPartitions still contains elements");
    }
  }
  /**
   * Generate full preference list from the master assignment map 
   * evenly distribute the slave partitions mastered on a host to other hosts
   *
   * @param masterAssignmentMap current master assignment map
   * @param orphanPartitions partitions not hosted by any instance
   * @return
   */
  Map<String, List<String>> generateListFieldFromMasterAssignment(Map<String, List<String>> masterAssignmentMap, int replicas)
  {
    Map<String, List<String>> listFields = new HashMap<String, List<String>>();
    int slaves = replicas - 1;
    String[] instanceNames = new String[masterAssignmentMap.size()];
    masterAssignmentMap.keySet().toArray(instanceNames);
    Arrays.sort(instanceNames);
    
    for(int i = 0; i < instanceNames.length; i++)
    {
      String instanceName = instanceNames[i];
      List<String> otherInstances = new ArrayList<String>(masterAssignmentMap.size() - 1);
      for(int x = 0 ; x < instanceNames.length - 1; x++)
      {
        int index = (x + i + 1) % instanceNames.length;
        otherInstances.add(instanceNames[index]);
      }
      
      List<String> partitionList = masterAssignmentMap.get(instanceName);
      for(int j = 0;j < partitionList.size(); j++)
      {
        String partitionName = partitionList.get(j);
        listFields.put(partitionName, new ArrayList<String>());
        listFields.get(partitionName).add(instanceName);
        
        for(int k = 0; k < slaves; k++)
        {
          int index = (j+k+1) % otherInstances.size();
          listFields.get(partitionName).add(otherInstances.get(index));
        }
      }
    }
    return listFields;
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
