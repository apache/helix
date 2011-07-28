package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

//import com.linkedin.clustermanager.ZNRecord;
/*
 * IdealStateCalculatorForStorageNode tries to optimally allocate master/slave partitions among 
 * espresso storage nodes.
 * 
 * Given a batch of storage nodes, the partition and replication factor, the algorithm first given a initial state
 * When new batches of storage nodes are added, the algorithm will calculate the new ideal state such that the total
 * partition movements are minimized.
 *  
 * */
public class IdealStateCalculatorForStorageNode
{
  /**
   * Build the config map for RUSH algorithm. The input of RUSH algorithm groups
   * nodes into "cluster"s, and different clusters can be assigned with
   * different weights.
   * 
   * 1. Calculate the master assignment by random shuffling
   * 2. for each storage instance, calculate the 1st slave assignment map, by another random shuffling
   * 3. for each storage instance, calculate the i-th slave assignment map
   * 4. Combine the i-th slave assignment maps together
   * 
   * @param instanceNames
   *          list of storage node instances
   * @param weight
   *          weight for the initial storage node (each node has the same weight)
   * @param partitions
   *          number of partitions
   * @param replicas
   *          The number of replicas (slave partitions) per master partition
   * @return a map that contain the idealstate info
   */
  public static Map<String, Object> calculateInitialIdealState(List<String> instanceNames, int weight, int partitions, int replicas)
  {
    Random r = new Random(123456);
    assert(replicas < instanceNames.size() - 1);
    
    ArrayList<Integer> masterPartitionAssignment = new ArrayList<Integer>();
    for(int i = 0;i< partitions; i++)
    {
      masterPartitionAssignment.add(i);
    }
    // shuffle the partition id array
    Collections.shuffle(masterPartitionAssignment, new Random(r.nextInt()));
    
    // 1. Generate the random master partition assignment 
    //    instanceName -> List of master partitions on that instance
    Map<String, List<Integer>> nodeMasterAssignmentMap = new TreeMap<String, List<Integer>>();
    for(int i = 0; i < masterPartitionAssignment.size(); i++)
    {
      String instanceName = instanceNames.get(i % instanceNames.size());
      if(!nodeMasterAssignmentMap.containsKey(instanceName))
      {
        nodeMasterAssignmentMap.put(instanceName, new ArrayList<Integer>());
      }
      nodeMasterAssignmentMap.get(instanceName).add(masterPartitionAssignment.get(i));
    }
    
    // instanceName -> slave assignment for its master partitions
    // slave assignment: instanceName -> list of slave partitions on it
    List<Map<String, Map<String, List<Integer>>>> nodeSlaveAssignmentMapsList = new ArrayList<Map<String, Map<String, List<Integer>>>>(replicas);
    
    Map<String, Map<String, List<Integer>>> firstNodeSlaveAssignmentMap = new TreeMap<String, Map<String, List<Integer>>>();
    
    // 2. For each node, calculate the evenly distributed slave as the first slave assignment
    // We will figure out the 2nd ...replicas-th slave assignment based on the first level slave assignment
    for(int i = 0; i < instanceNames.size(); i++)
    {
      List<String> slaveInstances = new ArrayList<String>();
      for(int j = 0;j < instanceNames.size(); j++)
      {
        if(j != i)
        {
          slaveInstances.add(instanceNames.get(j)); 
        }
      }
      // Get the number of master partitions on instanceName
      List<Integer> masterAssignment =  nodeMasterAssignmentMap.get(instanceNames.get(i));
      // do a random shuffling as in step 1, so that the first-level slave are distributed among rest instances
      ArrayList<Integer> slaveAssignment = new ArrayList<Integer>();
      for(int j = 0;j < masterAssignment.size(); j++)
      {
        slaveAssignment.add(j);
      }
      Collections.shuffle(slaveAssignment, new Random(r.nextInt()));
      
      TreeMap<String, List<Integer>> slaveAssignmentMap = new TreeMap<String, List<Integer>>();
      
      // Get the slave assignment map of node instanceName
      for(int j = 0;j < masterAssignment.size(); j++)
      {
        String slaveInstanceName = slaveInstances.get(slaveAssignment.get(j) % slaveInstances.size());
        if(!slaveAssignmentMap.containsKey(slaveInstanceName))
        {
          slaveAssignmentMap.put(slaveInstanceName, new ArrayList<Integer>());
        }
        slaveAssignmentMap.get(slaveInstanceName).add(masterAssignment.get(j));
      }
      firstNodeSlaveAssignmentMap.put(instanceNames.get(i), slaveAssignmentMap);
    }
    nodeSlaveAssignmentMapsList.add(firstNodeSlaveAssignmentMap);
    // From the first slave assignment map, calculate the rest slave assignment maps
    for(int replicaOrder = 1; replicaOrder < replicas; replicaOrder++)
    {
      // calculate the next slave partition assignment map
      Map<String, Map<String, List<Integer>>> nextNodeSlaveAssignmentMap 
        = calculateNextSlaveAssignemntMap(firstNodeSlaveAssignmentMap, replicaOrder);
      nodeSlaveAssignmentMapsList.add(nextNodeSlaveAssignmentMap);
    }
    
    // Combine the calculated 1...replicas-th slave assignment map together
    Map<String, Map<String, List<Integer>>> combinedNodeSlaveAssignmentMap = new TreeMap<String, Map<String, List<Integer>>>();
    
    for(String instanceName : nodeMasterAssignmentMap.keySet())
    {
      Map<String, List<Integer>> combinedSlaveAssignmentMap =  new TreeMap<String, List<Integer>>();
      
      for(Map<String, Map<String, List<Integer>>> slaveNodeAssignmentMap : nodeSlaveAssignmentMapsList)
      {
        Map<String, List<Integer>> slaveAssignmentMap = slaveNodeAssignmentMap.get(instanceName);
        
        for(String slaveInstance : slaveAssignmentMap.keySet())
        {
          if(!combinedSlaveAssignmentMap.containsKey(slaveInstance))
          {
            combinedSlaveAssignmentMap.put(slaveInstance, new ArrayList<Integer>());
          }
          combinedSlaveAssignmentMap.get(slaveInstance).addAll(slaveAssignmentMap.get(slaveInstance));
        }
      }
      migrateSlaveAssignMapToNewInstances(combinedSlaveAssignmentMap, new ArrayList<String>());
      combinedNodeSlaveAssignmentMap.put(instanceName, combinedSlaveAssignmentMap);
    }
    
    /*
    // Print the result master and slave assignment maps
    System.out.println("Master assignment:");
    for(String instanceName : nodeMasterAssignmentMap.keySet())
    {
      System.out.println(instanceName+":");
      for(Integer x : nodeMasterAssignmentMap.get(instanceName))
      {
        System.out.print(x+" ");
      }
      System.out.println();
      System.out.println("Slave assignment:");
      
      int slaveOrder = 1;
      for(Map<String, Map<String, List<Integer>>> slaveNodeAssignmentMap : nodeSlaveAssignmentMapsList)
      {
        System.out.println("Slave assignment order :" + (slaveOrder++));
        Map<String, List<Integer>> slaveAssignmentMap = slaveNodeAssignmentMap.get(instanceName);
        for(String slaveName : slaveAssignmentMap.keySet())
        {
          System.out.print("\t" + slaveName +":\n\t" );
          for(Integer x : slaveAssignmentMap.get(slaveName))
          {
            System.out.print(x + " ");
          }
          System.out.println("\n");
        }
      }
      System.out.println("\nCombined slave assignment map");
      Map<String, List<Integer>> slaveAssignmentMap = combinedNodeSlaveAssignmentMap.get(instanceName);
      for(String slaveName : slaveAssignmentMap.keySet())
      {
        System.out.print("\t" + slaveName +":\n\t" );
        for(Integer x : slaveAssignmentMap.get(slaveName))
        {
          System.out.print(x + " ");
        }
        System.out.println("\n");
      }
    }*/
    Map<String, Object> result = new TreeMap<String, Object>();
    result.put("MasterAssignmentMap", nodeMasterAssignmentMap);
    result.put("SlaveAssignmentMap", combinedNodeSlaveAssignmentMap);
    result.put("replicas", new Integer(replicas));
    result.put("partitions", new Integer(partitions));
    return result;
  }
  /**
   * In the case there are more than 1 slave, we use the following algorithm to calculate the n-th slave 
   * assignment map based on the first level slave assignment map.
   * 
   * @param firstInstanceSlaveAssignmentMap  the first slave assignment map for all instances
   * @param order of the slave
   * @return the n-th slave assignment map for all the instances
   * */
  public static Map<String, Map<String, List<Integer>>> calculateNextSlaveAssignemntMap(Map<String, Map<String, List<Integer>>> firstInstanceSlaveAssignmentMap, int replicaOrder)
  {
    Map<String, Map<String, List<Integer>>> result = new TreeMap<String, Map<String, List<Integer>>>();
    
    for(String currentInstance : firstInstanceSlaveAssignmentMap.keySet())
    {
      Map<String, List<Integer>> resultAssignmentMap = new TreeMap<String, List<Integer>>();
      result.put(currentInstance, resultAssignmentMap);
    }
    
    for(String currentInstance : firstInstanceSlaveAssignmentMap.keySet())
    {
      Map<String, List<Integer>> previousSlaveAssignmentMap = firstInstanceSlaveAssignmentMap.get(currentInstance);
      Map<String, List<Integer>> resultAssignmentMap = result.get(currentInstance);
      int offset = replicaOrder - 1;
      for(String instance : previousSlaveAssignmentMap.keySet())
      {
        List<String> otherInstances = new ArrayList<String>(previousSlaveAssignmentMap.size() - 1);
        // Obtain an array of other instances
        for(String otherInstance : previousSlaveAssignmentMap.keySet())
        {
          otherInstances.add(otherInstance);
        }
        Collections.sort(otherInstances);
        int instanceIndex = -1;
        for(int index = 0;index < otherInstances.size(); index++)
        {
          if(otherInstances.get(index).equalsIgnoreCase(instance))
          {
            instanceIndex = index;
          }
        }
        assert(instanceIndex >= 0);
        if(instanceIndex == otherInstances.size() - 1)
        {
          instanceIndex --;
        }
        // Since we need to evenly distribute the slaves on "instance" to other partitions, we 
        // need to remove "instance" from the array.
        otherInstances.remove(instance);
        
        // distribute previous slave assignment to other instances. 
        List<Integer> previousAssignmentList = previousSlaveAssignmentMap.get(instance);
        for(int i = 0; i < previousAssignmentList.size(); i++)
        {
          
          // Evenly distribute the previousAssignmentList to the remaining other instances
          int newInstanceIndex = (i + offset + instanceIndex) % otherInstances.size();
          String newInstance = otherInstances.get(newInstanceIndex);
          if(!resultAssignmentMap.containsKey(newInstance))
          {
            resultAssignmentMap.put(newInstance, new ArrayList<Integer>());
          }
          resultAssignmentMap.get(newInstance).add(previousAssignmentList.get(i));
        }
      }
    }
    return result;  
  }
  
  /**
   * Given the current idealState, and the list of new Instances needed to be added, calculate the 
   * new Ideal state.
   * 
   * 1. Calculate how many master partitions should be moved to the new cluster of instances
   * 2. assign the number of master partitions px to be moved to each previous node
   * 3. for each previous node, 
   *    3.1 randomly choose px nodes, move them to temp list
   *    3.2 for each px nodes, remove them from the slave assignment map; record the map position of 
   *        the partition;
   *    3.3 calculate # of new nodes that should be put in the slave assignment map
   *    3.4 even-fy the slave assignment map;
   *    3.5 randomly place # of new nodes that should be placed in
   * 
   * 4. from all the temp master node list get from 3.1,
   *    4.1 randomly assign them to nodes in the new cluster
   *    
   * 5. for each node in the new cluster, 
   *    5.1 assemble the slave assignment map
   *    5.2 even-fy the slave assignment map
   *    
   * @param newInstances
   *          list of new added storage node instances
   * @param weight
   *          weight for the new storage nodes (each node has the same weight)
   * @param previousIdealState
   *          The previous ideal state
   * @return a map that contain the updated idealstate info
   * */
  public static Map<String, Object> calculateNextIdealState(List<String> newInstances, int weight, Map<String, Object> previousIdealState)
  {
    // Obtain the master / slave assignment info maps
    Map<String, List<Integer>> previousMasterAssignmentMap 
        = (Map<String, List<Integer>>) (previousIdealState.get("MasterAssignmentMap"));
    Map<String, Map<String, List<Integer>>> nodeSlaveAssignmentMap 
        = (Map<String, Map<String, List<Integer>>>)(previousIdealState.get("SlaveAssignmentMap"));
    
    List<String> oldInstances = new ArrayList<String>();
    for(String oldInstance : previousMasterAssignmentMap.keySet())
    {
      oldInstances.add(oldInstance);
    }
    
    int previousInstanceNum = previousMasterAssignmentMap.size();
    int partitions = (Integer)(previousIdealState.get("partitions"));
    
    // TODO: take weight into account when calculate this
    
    int totalMasterParitionsToMove 
        = partitions * (newInstances.size()) / (previousInstanceNum + newInstances.size());
    int numMastersFromEachNode = totalMasterParitionsToMove / previousInstanceNum;
    int remain = totalMasterParitionsToMove % previousInstanceNum;
    
    // Note that when remain > 0, we should make [remain] moves with (numMastersFromEachNode + 1) partitions. 
    // And we should first choose those (numMastersFromEachNode + 1) moves from the instances that has more 
    // master partitions
    List<Integer> masterPartitionListToMove = new ArrayList<Integer>();
    
    // For corresponding moved slave partitions, keep track of their original location; the new node does not 
    // need to migrate all of them.
    Map<String, List<Integer>> slavePartitionsToMoveMap = new TreeMap<String, List<Integer>>();
    
    // Make sure that the instances that holds more master partitions are put in front
    List<String> bigList = new ArrayList<String>(), smallList = new ArrayList<String>();
    for(String oldInstance : previousMasterAssignmentMap.keySet())
    {
      List<Integer> masterAssignmentList = previousMasterAssignmentMap.get(oldInstance);
      if(masterAssignmentList.size() > numMastersFromEachNode)
      {
        bigList.add(oldInstance);
      }
      else
      {
        smallList.add(oldInstance);
      }
    }
    // "sort" the list, such that the nodes that has more master partitions moves more partitions to the 
    // new added batch of instances.
    bigList.addAll(smallList);
    int totalSlaveMoves = 0;
    for(String oldInstance : bigList)
    {
      List<Integer> masterAssignmentList = previousMasterAssignmentMap.get(oldInstance);
      int numToChoose = numMastersFromEachNode;
      if(remain > 0)
      {
        numToChoose = numMastersFromEachNode + 1;
        remain --;
      }
      // randomly remove numToChoose of master partitions to the new added nodes
      ArrayList<Integer> masterPartionsMoved = new ArrayList<Integer>();
      randomSelect(masterAssignmentList, masterPartionsMoved, numToChoose);
 
      masterPartitionListToMove.addAll(masterPartionsMoved);
      Map<String, List<Integer>> slaveAssignmentMap = nodeSlaveAssignmentMap.get(oldInstance);
      removeFromSlaveAssignmentMap(slaveAssignmentMap, masterPartionsMoved, slavePartitionsToMoveMap);
      
      // Make sure that for old instances, the slave placement map is evenly distributed
      // Trace the "local slave moves", which should together contribute to most of the slave migrations
      int movesWithinInstance = migrateSlaveAssignMapToNewInstances(slaveAssignmentMap, newInstances);
      // System.out.println("local moves: "+ movesWithinInstance);
      totalSlaveMoves += movesWithinInstance;
    }
    // System.out.println("local slave moves total: "+ totalSlaveMoves);
    // calculate the master /slave assignment for the new added nodes
    
    // We already have the list of master partitions that will migrate to new batch of instances, 
    // shuffle the partitions and assign them to new instances
    Collections.shuffle(masterPartitionListToMove, new Random(12345));
    for(int i = 0;i < newInstances.size(); i++)
    {
      String newInstance = newInstances.get(i);
      List<Integer> masterPartitionList = new ArrayList<Integer>();
      for(int j = 0;j < masterPartitionListToMove.size(); j++)
      {
        if(j % newInstances.size() == i)
        {
          masterPartitionList.add(masterPartitionListToMove.get(j));
        }
      }
      
      Map<String, List<Integer>> slavePartitionMap = new TreeMap<String, List<Integer>>();
      for(String oldInstance : oldInstances)
      {
        slavePartitionMap.put(oldInstance, new ArrayList<Integer>());
      }
      // Build the slave assignment map for the new instance, based on the saved information
      // about those slave partition locations in slavePartitionsToMoveMap
      for(Integer x : masterPartitionList)
      {
        for(String oldInstance : slavePartitionsToMoveMap.keySet())
        {
          List<Integer> slaves = slavePartitionsToMoveMap.get(oldInstance);
          if(slaves.contains(x))
          {
            slavePartitionMap.get(oldInstance).add(x);
          }
        }
      }
      // add entry for other new instances into the slavePartitionMap
      List<String> otherNewInstances = new ArrayList<String>();
      for(String instance : newInstances)
      {
        if(!instance.equalsIgnoreCase(newInstance))
        {
          otherNewInstances.add(instance);
        }
      }
      // Make sure that slave partitions are evenly distributed
      migrateSlaveAssignMapToNewInstances(slavePartitionMap, otherNewInstances);
      
      // Update the result in the result map. We can reuse the input previousIdealState map as 
      // the result.
      previousMasterAssignmentMap.put(newInstance, masterPartitionList);
      nodeSlaveAssignmentMap.put(newInstance, slavePartitionMap);
      
    }
    /*
    // Print content of the master/ slave assignment maps
    for(String instanceName : previousMasterAssignmentMap.keySet())
    {
      System.out.println(instanceName+":");
      for(Integer x : previousMasterAssignmentMap.get(instanceName))
      {
        System.out.print(x+" ");
      }
      System.out.println("\nmaster partition moved:");
      
      System.out.println();
      System.out.println("Slave assignment:");
      
      Map<String, List<Integer>> slaveAssignmentMap = nodeSlaveAssignmentMap.get(instanceName);
      for(String slaveName : slaveAssignmentMap.keySet())
      {
        System.out.print("\t" + slaveName +":\n\t" );
        for(Integer x : slaveAssignmentMap.get(slaveName))
        {
          System.out.print(x + " ");
        }
        System.out.println("\n");
      }
    }
    
    System.out.println("Master partitions migrated to new instances");
    for(Integer x : masterPartitionListToMove)
    {
        System.out.print(x+" ");
    }
    System.out.println();
    
    System.out.println("Slave partitions migrated to new instances");
    for(String oldInstance : slavePartitionsToMoveMap.keySet())
    {
        System.out.print(oldInstance + ": ");
        for(Integer x : slavePartitionsToMoveMap.get(oldInstance))
        {
          System.out.print(x+" ");
        }
        System.out.println();
    }
    */
    return previousIdealState;
  }
  
  /**
   * Given the list of master partition that will be migrated away from the storage instance,
   * Remove their entries from the local instance slave assignment map.
   * 
   * @param slaveAssignmentMap  the local instance slave assignment map
   * @param masterPartionsMoved the list of master partition ids that will be migrated away
   * @param removedAssignmentMap keep track of the removed slave assignment info. The info can be 
   *        used by new added storage nodes.
   * */
  public static void removeFromSlaveAssignmentMap( Map<String, List<Integer>>slaveAssignmentMap, List<Integer> masterPartionsMoved, Map<String, List<Integer>> removedAssignmentMap)
  {
    for(String instanceName : slaveAssignmentMap.keySet())
    {
      List<Integer> slaveAssignment = slaveAssignmentMap.get(instanceName);
      for(Integer partitionId: masterPartionsMoved)
      {
        if(slaveAssignment.contains(partitionId))
        {
          slaveAssignment.remove(partitionId);
          if(!removedAssignmentMap.containsKey(instanceName))
          {
            removedAssignmentMap.put(instanceName, new ArrayList<Integer>());
          }
          removedAssignmentMap.get(instanceName).add(partitionId);
        }
      }
    }
  }
  
  /**
   * Since some new storage instances are added, each existing storage instance should migrate some 
   * slave partitions to the new added instances.
   * 
   * The algorithm keeps moving one partition to from the instance that hosts most slave partitions 
   * to the instance that hosts least number of partitions, until max-min <= 1.
   * 
   * In this way we can guarantee that all instances hosts almost same number of slave partitions, also
   * slave partitions are evenly distributed.
   * 
   * @param slaveAssignmentMap  the local instance slave assignment map
   * @param masterPartionsMoved the list of master partition ids that will be migrated away
   * @param removedAssignmentMap keep track of the removed slave assignment info. The info can be 
   *        used by new added storage nodes.
   * */
  public static int migrateSlaveAssignMapToNewInstances(Map<String, List<Integer>> nodeSlaveAssignmentMap, List<String> newInstances)
  {
    int moves = 0;
    boolean done = false;
    for(String newInstance : newInstances)
    {
      nodeSlaveAssignmentMap.put(newInstance, new ArrayList<Integer>());
    }
    while(!done)
    {
      List<Integer> maxAssignment = null, minAssignment = null;
      int minCount = Integer.MAX_VALUE, maxCount = Integer.MIN_VALUE;
      String minInstance = "";
      for(String instanceName : nodeSlaveAssignmentMap.keySet())
      {
        List<Integer> slaveAssignment = nodeSlaveAssignmentMap.get(instanceName);
        if(minCount > slaveAssignment.size())
        {
          minCount = slaveAssignment.size();
          minAssignment = slaveAssignment;
          minInstance = instanceName;
        }
        if(maxCount < slaveAssignment.size())
        {
          maxCount = slaveAssignment.size();
          maxAssignment = slaveAssignment;
        }
      }
      if(maxCount - minCount <= 1 )
      {
        done = true;
      }
      else
      {
        int indexToMove = -1;
        // find a partition that is not contained in the minAssignment list
        for(int i = 0; i < maxAssignment.size(); i++ )
        {
          if(!minAssignment.contains(maxAssignment.get(i)))
          {
            indexToMove = i;
            break;
          }
        }
         
        minAssignment.add(maxAssignment.get(indexToMove));
        maxAssignment.remove(indexToMove);
        
        if(newInstances.contains(minInstance))
        {
          moves++;
        }
      }
    }
    return moves;
  }
  
  /**
   * Randomly select a number of elements from original list and put them in the selectedList
   * The algorithm is used to select master partitions to be migrated when new instances are added.
   *
   * 
   * @param originalList  the original list
   * @param selectedList  the list that contain selected elements
   * @param num number of elements to be selected
   * */
  public static void randomSelect(List<Integer> originalList, List<Integer> selectedList, int num)
  {
    assert(originalList.size() >= num);
    int[] indexArray = new int[originalList.size()];
    for(int i = 0;i < indexArray.length; i++)
    {
      indexArray[i] = i;
    }
    int numRemains = originalList.size();
    Random r = new Random(numRemains);
    for(int j = 0;j < num; j++)
    {
      int randIndex = r.nextInt(numRemains);
      selectedList.add(originalList.get(randIndex));
      originalList.remove(randIndex);
      numRemains --; 
    }
  }
  
  public static int migrateSlaveAssignMapToNewInstancesWithWeights(
      Map<String, List<Integer>> nodeSlaveAssignmentMap, 
      List<String> newInstances, 
      Map<String, Double> existingInstanceWeights, 
      double newInstanceWeight
      )
  {
    int moves = 0;
    boolean done = false;
    for(String newInstance : newInstances)
    {
      nodeSlaveAssignmentMap.put(newInstance, new ArrayList<Integer>());
    }
    
    
    
    while(!done)
    {
      List<Integer> maxAssignment = null, minAssignment = null;
      int minCount = Integer.MAX_VALUE, maxCount = Integer.MIN_VALUE;
      String minInstance = "";
      for(String instanceName : nodeSlaveAssignmentMap.keySet())
      {
        List<Integer> slaveAssignment = nodeSlaveAssignmentMap.get(instanceName);
        if(minCount > slaveAssignment.size())
        {
          minCount = slaveAssignment.size();
          minAssignment = slaveAssignment;
          minInstance = instanceName;
        }
        if(maxCount < slaveAssignment.size())
        {
          maxCount = slaveAssignment.size();
          maxAssignment = slaveAssignment;
        }
      }
      if(maxCount - minCount <= 1 )
      {
        done = true;
      }
      else
      {
        int indexToMove = -1;
        // find a partition that is not contained in the minAssignment list
        for(int i = 0; i < maxAssignment.size(); i++ )
        {
          if(!minAssignment.contains(maxAssignment.get(i)))
          {
            indexToMove = i;
            break;
          }
        }
         
        minAssignment.add(maxAssignment.get(indexToMove));
        maxAssignment.remove(indexToMove);
        
        if(newInstances.contains(minInstance))
        {
          moves++;
        }
      }
    }
    return moves;
  }
  
  // distribute the partitions list to assignments according to the designated instanceWeights
  // Used when calculating ini
  public void assignPartitionListToWeightedInstances(List<Integer> partitions, List<Integer> instanceWeights, List<List<Integer>> assignments)
  {
    List<Integer> numPartitionsAssignedList = new ArrayList<Integer>(instanceWeights);
    
    int totalWeights = 0;
    for(Integer weight: instanceWeights)
    {
      totalWeights += weight;
    }
    int remainderPartitions = partitions.size();
    for(Integer weight: instanceWeights)
    {
      int partitionAssigned  = partitions.size() * weight / totalWeights;
      remainderPartitions -= partitionAssigned;
      numPartitionsAssignedList.add(weight);
    }
    
    // take care of the "remainder" partitions, distribute them evenly
    for(int i = 0; i < remainderPartitions; i++)
    {
      int assigned = numPartitionsAssignedList.get(i % numPartitionsAssignedList.size());
      numPartitionsAssignedList.set(i % numPartitionsAssignedList.size(), assigned + 1);
    }
    
    int previousAssigned = 0;
    for(int i = 0; i < numPartitionsAssignedList.size(); i++)
    {
      Integer numPartitionsAssigned = numPartitionsAssignedList.get(i);
      List<Integer> assignedList = assignments.get(i);
      
      for(int j = previousAssigned; j < previousAssigned + numPartitionsAssigned; j++)
      {
        assignedList.add(partitions.get(j));
      }
      previousAssigned += numPartitionsAssigned;
    }
  }

  // TODO: implement this
  public Map<String, Object> calculateIdealState(List<List<String>>nodeBatches, List<Integer> weights, int partitions, int replicas)
  {
    return null;
  }
  
  
  
  public static void main(String args[])
  {
    List<String> instanceNames = new ArrayList<String>();
    for(int i = 0;i < 10; i++)
    {
      instanceNames.add("localhost:123" + i);
    }
    int partitions = 48*3, replicas = 3;
    Map<String, Object> resultOriginal = IdealStateCalculatorForStorageNode.calculateInitialIdealState(instanceNames,1, partitions, replicas);
    
  }
}
