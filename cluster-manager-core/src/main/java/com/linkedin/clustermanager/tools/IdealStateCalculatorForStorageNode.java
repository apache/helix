package com.linkedin.clustermanager.tools;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

//import com.linkedin.clustermanager.ZNRecord;

public class IdealStateCalculatorForStorageNode
{
  public static Map<String, Object> calculateInitialIdealState(List<String> instanceNames, int weight, int partitions, int replicas)
  {
    Random r = new Random(123456);
    assert(replicas < instanceNames.size() - 1);
    
    ArrayList<Integer> masterPartitionAssignment = new ArrayList<Integer>();
    for(int i = 0;i< partitions; i++)
    {
      masterPartitionAssignment.add(i);
    }
    Collections.shuffle(masterPartitionAssignment, new Random(r.nextInt()));
    
    // Generate the random master partition assignment 
    // NodeName -> List of master partitions
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
    
    // Nodename -> slave assignment for its master partitions
    //          node id -> list of partitions
    List<Map<String, Map<String, List<Integer>>>> nodeSlaveAssignmentMapsList = new ArrayList<Map<String, Map<String, List<Integer>>>>(replicas);
    Map<String, Map<String, List<Integer>>> firstNodeSlaveAssignmentMap = new TreeMap<String, Map<String, List<Integer>>>();
    
    // For each node, calculate the evenly distributed slave as the first slave assignment
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
      // 1st - order slave assignment
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
      Map<String, Map<String, List<Integer>>> nextNodeSlaveAssignmentMap 
        = calculateNextSlaveAssignemntMap(firstNodeSlaveAssignmentMap, replicaOrder);
      nodeSlaveAssignmentMapsList.add(nextNodeSlaveAssignmentMap);
    }
    
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
      combinedNodeSlaveAssignmentMap.put(instanceName, combinedSlaveAssignmentMap);
    }
      
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
    }
    Map<String, Object> result = new TreeMap<String, Object>();
    result.put("MasterAssignmentMap", nodeMasterAssignmentMap);
    result.put("SlaveAssignmentMap", combinedNodeSlaveAssignmentMap);
    result.put("replicas", new Integer(replicas));
    result.put("partitions", new Integer(partitions));
    return result;
  }
  /*
   * 1. Calculate how many master partitions should be moved to the new cluster of instances
   * 2. assign the number of master partitions px to be moved to each previous node
   * 3. for each previous node, 
   *    3.1 randomly choose px nodes, move them to temp list
   *    3.2 for each px nodes, remove them from the slave assignment map; record the map position of the partition;
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
   * */
  public static Map<String, Object> calculateNextIdealState(List<String> newInstances, int weights, Map<String, Object> previousIdealState)
  {
    // Now assume that all weights are the same
     
    Map<String, List<Integer>> previousMasterAssignmentMap = (Map<String, List<Integer>>) (previousIdealState.get("MasterAssignmentMap"));
    Map<String, Map<String, List<Integer>>> nodeSlaveAssignmentMap = (Map<String, Map<String, List<Integer>>>)(previousIdealState.get("SlaveAssignmentMap"));
    
    List<String> oldInstances = new ArrayList<String>();
    for(String oldInstance : previousMasterAssignmentMap.keySet())
    {
      oldInstances.add(oldInstance);
    }
    
    int previousInstanceNum = previousMasterAssignmentMap.size();
    int partitions = (Integer)(previousIdealState.get("partitions"));
    // Todo: take weight into account
    int totalMasterParitionsToMove = partitions * (newInstances.size()) / (previousInstanceNum + newInstances.size());
    
    int numMastersFromEachNode = totalMasterParitionsToMove / previousInstanceNum;
    int remain1 = totalMasterParitionsToMove % previousInstanceNum;

    List<Integer> masterPartitionListToMove = new ArrayList<Integer>();
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
    bigList.addAll(smallList);
    int totalSlaveMoves1 = 0;
    for(String oldInstance : bigList)
    {
      List<Integer> masterAssignmentList = previousMasterAssignmentMap.get(oldInstance);
      int numToChoose = numMastersFromEachNode;
      if(remain1 > 0)
      {
        numToChoose = numMastersFromEachNode + 1;
        remain1 --;
      }
      // randomly remove numToChoose of master partitions to the new added nodes
      ArrayList<Integer> masterPartionsMoved = new ArrayList<Integer>();
      randomSelect(masterAssignmentList, masterPartionsMoved, numToChoose);
 
      masterPartitionListToMove.addAll(masterPartionsMoved);
      Map<String, List<Integer>> slaveAssignmentMap = nodeSlaveAssignmentMap.get(oldInstance);
      removeFromSlaveAssignmentMap(slaveAssignmentMap, masterPartionsMoved, slavePartitionsToMoveMap);
      
      // Make sure that for old instances, the slave placement map is evenly distributed
      
      int moves1 = migrateSlaveAssignMapToNewNodes(slaveAssignmentMap, newInstances);
      System.out.println("local moves: "+ moves1);
      totalSlaveMoves1 += moves1;
    }
    System.out.println("local moves total1: "+ totalSlaveMoves1);
    // calculate the master /slave assignment for the new added nodes
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
      // Build the slave assignment map for the new instance
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
      // add entry for other new instances
      List<String> otherNewInstances = new ArrayList<String>();
      for(String instance : newInstances)
      {
        if(!instance.equalsIgnoreCase(newInstance))
        {
          otherNewInstances.add(instance);
        }
      }
      migrateSlaveAssignMapToNewNodes(slavePartitionMap, otherNewInstances);
      
      previousMasterAssignmentMap.put(newInstance, masterPartitionList);
      nodeSlaveAssignmentMap.put(newInstance, slavePartitionMap);
      
    }
    /*
    // Print content
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
    return null;
  }
  
  public static void removeFromSlaveAssignmentMap( Map<String, List<Integer>>slaveAssignmentMap, List<Integer> masterPartionsMoved, Map<String, List<Integer>> removedAssignmentMap)
  {
    for(String instanceName : slaveAssignmentMap.keySet())
    {
      List<Integer> assignment = slaveAssignmentMap.get(instanceName);
      for(Integer x: masterPartionsMoved)
      {
        if(assignment.contains(x))
        {
          assignment.remove(x);
          if(!removedAssignmentMap.containsKey(instanceName))
          {
            removedAssignmentMap.put(instanceName, new ArrayList<Integer>());
          }
          removedAssignmentMap.get(instanceName).add(x);
        }
      }
    }
  }
  
  public static void removeFromSlaveAssignmentMap( Map<String, List<Integer>>slaveAssignmentMap, List<Integer> masterPartionsMoved, Map<String, List<Integer>> removedAssignmentMap, int count)
  {
    int instanceNum = slaveAssignmentMap.size();
    int partitiontoSelect = count / instanceNum;
    int remainder = count % instanceNum;
    
    for(String instanceName : slaveAssignmentMap.keySet())
    {
      List<Integer> assignment = slaveAssignmentMap.get(instanceName);
      for(Integer x: masterPartionsMoved)
      {
        if(assignment.contains(x))
        {
          assignment.remove(x);
          if(!removedAssignmentMap.containsKey(instanceName))
          {
            removedAssignmentMap.put(instanceName, new ArrayList<Integer>());
          }
          removedAssignmentMap.get(instanceName).add(x);
        }
      }
    }
  }
  // randomly select num of slave partitions and assign them to the new nodes
  public static int migrateSlaveAssignMapToNewNodes(Map<String, List<Integer>> nodeSlaveAssignmentMap, List<String> newNodes)
  {
    int moves = 0;
    boolean done = false;
    for(String newInstance : newNodes)
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
        List<Integer> assignment = nodeSlaveAssignmentMap.get(instanceName);
        if(minCount > assignment.size())
        {
          minCount = assignment.size();
          minAssignment = assignment;
          minInstance = instanceName;
        }
        if(maxCount < assignment.size())
        {
          maxCount = assignment.size();
          maxAssignment = assignment;
        }
      }
      if(maxCount - minCount <= 1 )
      {
        done = true;
      }
      else
      {
        int indexToMove = -1;
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
        //if(newNodes.contains(minInstance))
        moves++;
      }
    }
    return moves;
  }
  
  // Randomly select num of primary partitions and put them in the selectedList
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
  
  public Map<String, Object> calculateIdealState(List<List<String>>nodeBatches, List<Integer> weights, int partitions, int replicas)
  {
    return null;
  }
  
  public List<List<Integer>> calculateSlaveMappingTable(int partitions, int replicas)
  {
    List<List<Integer>> result = new ArrayList<List<Integer>>();
    List<Integer> base = new ArrayList<Integer>(replicas);
    for(int i = 0;i < replicas; i++)
    {
      
    }
    
    return result;
  }
  // Order: 2...replicas
  public static Map<String, Map<String, List<Integer>>> calculateNextSlaveAssignemntMap(Map<String, Map<String, List<Integer>>> previousNodeSlaveAssignmentMap, int replicaOrder)
  {
    Map<String, Map<String, List<Integer>>> result = new TreeMap<String, Map<String, List<Integer>>>();
    
    for(String currentInstance : previousNodeSlaveAssignmentMap.keySet())
    {
      Map<String, List<Integer>> resultAssignmentMap = new TreeMap<String, List<Integer>>();
      result.put(currentInstance, resultAssignmentMap);
    }
    
    for(String currentInstance : previousNodeSlaveAssignmentMap.keySet())
    {
      Map<String, List<Integer>> previousSlaveAssignmentMap = previousNodeSlaveAssignmentMap.get(currentInstance);
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
        otherInstances.remove(instance);
        
        // distribute previous slave assignment to other instances
        List<Integer> previousAssignmentList = previousSlaveAssignmentMap.get(instance);
        for(int i = 0;i<previousAssignmentList.size(); i++)
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
      //result.put(currentInstance, resultAssignmentMap);
    }
    return result;  
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
