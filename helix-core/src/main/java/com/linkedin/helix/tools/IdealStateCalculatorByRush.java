package com.linkedin.helix.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.model.IdealState.IdealStateProperty;

public class IdealStateCalculatorByRush
{
  /**
   * Build the config map for RUSH algorithm. The input of RUSH algorithm groups
   * nodes into "cluster"s, and different clusters can be assigned with
   * different weights.
   *
   * @param numClusters
   *          number of node clusters
   * @param instancesPerCluster
   *          List of clusters, each contain a list of node name strings.
   * @param replicationDegree
   *          the replication degree
   * @param clusterWeights
   *          the weight for each node cluster
   * @return this config map structure for RUSH algorithm.
   */
  static HashMap<String, Object> buildRushConfig(int numClusters,
      List<List<String>> instancesPerCluster, int replicationDegree,
      List<Integer> clusterWeights)
  {
    HashMap<String, Object> config = new HashMap<String, Object>();
    config.put("replicationDegree", replicationDegree);

    HashMap[] clusterList = new HashMap[numClusters];
    config.put("subClusters", clusterList);

    HashMap[] nodes;
    HashMap<String, String> node;
    HashMap<String, Object> clusterData;
    for (int n = 0; n < numClusters; n++)
    {
      int numNodes = instancesPerCluster.get(n).size();
      nodes = new HashMap[numNodes];
      for (int i = 0; i < numNodes; i++)
      {
        node = new HashMap<String, String>();
        node.put("partition", instancesPerCluster.get(n).get(i));
        nodes[i] = node;
      }
      clusterData = new HashMap<String, Object>();
      clusterData.put("weight", clusterWeights.get(n));
      clusterData.put("nodes", nodes);
      clusterList[n] = clusterData;
    }
    return config;
  }

  /**
   * Calculate the ideal state for list of instances clusters.
   *
   * @param numClusters
   *          number of node clusters
   * @param instanceClusters
   *          List of clusters, each contain a list of node name strings.
   * @param instanceClusterWeights
   *          the weight for each instance cluster
   * @param partitions
   *          the partition number of the database
   * @param replicas
   *          the replication degree
   * @param dbName
   *          the name of the database
   * @return The ZNRecord that contains the ideal state
   */
  public static ZNRecord calculateIdealState(
      List<List<String>> instanceClusters,
      List<Integer> instanceClusterWeights, int partitions, int replicas,
      String dbName) throws Exception
  {
    ZNRecord result = new ZNRecord(dbName);

    int numberOfClusters = instanceClusters.size();
    List<List<String>> nodesInClusters = instanceClusters;
    List<Integer> clusterWeights = instanceClusterWeights;

    HashMap<String, Object> rushConfig = buildRushConfig(numberOfClusters,
        nodesInClusters, replicas + 1, clusterWeights);
    RUSHrHash rushHash = new RUSHrHash(rushConfig);

    Random r = new Random(0);
    for (int i = 0; i < partitions; i++)
    {
      int partitionId = i;
      String partitionName = dbName + ".partition-" + partitionId;

      ArrayList<HashMap> partitionAssignmentResult = rushHash
          .findNode(i);
      List<String> nodeNames = new ArrayList<String>();
      for (HashMap p : partitionAssignmentResult)
      {
        for (Object key : p.keySet())
        {
          if (p.get(key) instanceof String)
          {
            nodeNames.add(p.get(key).toString());
          }
        }
      }
      Map<String, String> partitionAssignment = new TreeMap<String, String>();

      for (int j = 0; j < nodeNames.size(); j++)
      {
        partitionAssignment.put(nodeNames.get(j), "SLAVE");
      }
      int master = r.nextInt(nodeNames.size());
      //master = nodeNames.size()/2;
      partitionAssignment.put(nodeNames.get(master), "MASTER");


      result.setMapField(partitionName, partitionAssignment);
    }
    result.setSimpleField(IdealStateProperty.RESOURCES.toString(), String.valueOf(partitions));
    return result;
  }

  public static ZNRecord calculateIdealState(
      List<String> instanceClusters,
      int instanceClusterWeight, int partitions, int replicas,
      String dbName) throws Exception
  {
    List<List<String>> instanceClustersList = new ArrayList<List<String>>();
    instanceClustersList.add(instanceClusters);

    List<Integer> instanceClusterWeightList = new ArrayList<Integer>();
    instanceClusterWeightList.add(instanceClusterWeight);

    return calculateIdealState(
        instanceClustersList,
        instanceClusterWeightList, partitions, replicas,
        dbName);
  }
  /**
   * Helper function to see how many partitions are mapped to different
   * instances in two ideal states
   * */
  public static void printDiff(ZNRecord record1, ZNRecord record2)
  {
    int diffCount = 0;
    int diffCountMaster = 0;
    for (String key : record1.getMapFields().keySet())
    {
      Map<String, String> map1 = record1.getMapField(key);
      Map<String, String> map2 = record2.getMapField(key);

      for (String k : map1.keySet())
      {
        if (!map2.containsKey(k))
        {
          diffCount++;
        }
        else if (!map1.get(k).equalsIgnoreCase(map2.get(k)))
        {
          diffCountMaster++;
        }
      }
    }
    System.out.println("\ndiff count = " + diffCount);
    System.out.println("\nmaster diff count:"+ diffCountMaster);
  }

  /**
   * Helper function to calculate and print the standard deviation of the
   * partition assignment ideal state.
   * */
  public static void printIdealStateStats(ZNRecord record)
  {
    Map<String, Integer> countsMap = new TreeMap<String, Integer>();
    Map<String, Integer> masterCountsMap = new TreeMap<String, Integer>();
    for (String key : record.getMapFields().keySet())
    {
      Map<String, String> map1 = record.getMapField(key);
      for (String k : map1.keySet())
      {
        if (!countsMap.containsKey(k))
        {
          countsMap.put(k, new Integer(0));
        }
        else
        {
          countsMap.put(k, countsMap.get(k).intValue() + 1);
        }
        if (!masterCountsMap.containsKey(k))
        {
          masterCountsMap.put(k, new Integer(0));

        }
        else if (map1.get(k).equalsIgnoreCase("MASTER"))
        {
          masterCountsMap.put(k, masterCountsMap.get(k).intValue() + 1);
        }
      }
    }
    double sum = 0;
    int maxCount = 0;
    int minCount = Integer.MAX_VALUE;
    for (String k : countsMap.keySet())
    {
      int count = countsMap.get(k);
      sum += count;
      if (maxCount < count)
      {
        maxCount = count;
      }
      if (minCount > count)
      {
        minCount = count;
      }
      System.out.print(count + " ");
    }
    System.out.println("\nMax count: " + maxCount + " min count:" + minCount);
    System.out.println("\n master:");
    double sumMaster = 0;
    int maxCountMaster = 0;
    int minCountMaster = Integer.MAX_VALUE;
    for (String k : masterCountsMap.keySet())
    {
      int count = masterCountsMap.get(k);
      sumMaster += count;
      if (maxCountMaster < count)
      {
        maxCountMaster = count;
      }
      if (minCountMaster > count)
      {
        minCountMaster = count;
      }
      System.out.print(count + " ");
    }
    System.out.println("\nMean master: "+ 1.0*sumMaster/countsMap.size());
    System.out.println("Max master count: " + maxCountMaster + " min count:" + minCountMaster);
    double mean = sum / (countsMap.size());
    // calculate the deviation of the node distribution
    double deviation = 0;
    for (String k : countsMap.keySet())
    {
      double count = countsMap.get(k);
      deviation += (count - mean) * (count - mean);
    }
    System.out.println("Mean: " + mean + " normal deviation:"
        + Math.sqrt(deviation / countsMap.size()) / mean);

    //System.out.println("Max count: " + maxCount + " min count:" + minCount);
    int steps = 10;
    int stepLen = (maxCount - minCount) / steps;
    if(stepLen == 0) return;
    List<Integer> histogram = new ArrayList<Integer>((maxCount - minCount)
        / stepLen + 1);

    for (int i = 0; i < (maxCount - minCount) / stepLen + 1; i++)
    {
      histogram.add(0);
    }
    for (String k : countsMap.keySet())
    {
      int count = countsMap.get(k);
      int stepNo = (count - minCount) / stepLen;
      histogram.set(stepNo, histogram.get(stepNo) + 1);
    }
    System.out.println("histogram:");
    for (Integer x : histogram)
    {
      System.out.print(x + " ");
    }
  }

  public static void main(String args[]) throws Exception
  {
    int partitions = 4096, replicas = 2;
    String dbName = "espressoDB1";
    List<String> instanceNames = new ArrayList<String>();
    List<List<String>> instanceCluster1 = new ArrayList<List<String>>();
    for (int i = 0; i < 20; i++)
    {
      instanceNames.add("local"+i+"host_123" + i);
    }
    instanceCluster1.add(instanceNames);
    List<Integer> weights1 = new ArrayList<Integer>();
    weights1.add(1);
    ZNRecord result = IdealStateCalculatorByRush.calculateIdealState(
        instanceCluster1, weights1, partitions, replicas, dbName);

    printIdealStateStats(result);

    List<String> instanceNames2 = new ArrayList<String>();
    for (int i = 400; i < 405; i++)
    {
      instanceNames2.add("localhost_123" + i);
    }
    instanceCluster1.add(instanceNames2);
    weights1.add(1);
    ZNRecord result2 = IdealStateCalculatorByRush.calculateIdealState(
        instanceCluster1, weights1, partitions, replicas, dbName);

    printDiff(result, result2);
    printIdealStateStats(result2);
  }
}
