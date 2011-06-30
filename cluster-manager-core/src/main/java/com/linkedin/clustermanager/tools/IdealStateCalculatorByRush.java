package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;

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
    ZNRecord result = new ZNRecord();
    result.setId(dbName);

    int numberOfClusters = instanceClusters.size();
    List<List<String>> nodesInClusters = instanceClusters;
    List<Integer> clusterWeights = instanceClusterWeights;

    HashMap<String, Object> rushConfig = buildRushConfig(numberOfClusters,
        nodesInClusters, replicas + 1, clusterWeights);
    RUSHrHash rushHash = new RUSHrHash(rushConfig);

    for (int i = 0; i < partitions; i++)
    {
      int partitionId = i;
      String partitionName = dbName + ".partition-" + partitionId;

      ArrayList<HashMap> partitionAssignmentResult = rushHash
          .findNode(partitionName);
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
      // the first in the list is the node that contains the master
      partitionAssignment.put(nodeNames.get(0), "MASTER");
      // for the jth replica, we put it on (masterNode + j) % nodes-th node
      for (int j = 1; j < nodeNames.size(); j++)
      {
        partitionAssignment.put(nodeNames.get(j), "SLAVE");
      }

      result.setMapField(partitionName, partitionAssignment);
    }
    result.setSimpleField("partitions", String.valueOf(partitions));
    return result;
  }

  /**
   * Calculate the ideal state for list of instances. The function put all the
   * instance in one single cluster.
   * 
   * @param instanceNames
   *          List of instance names.
   * @param partitions
   *          the partition number of the database
   * @param replicas
   *          the replication degree
   * @param dbName
   *          the name of the database
   * @return The ZNRecord that contains the ideal state
   */
  public static ZNRecord calculateIdealState(List<String> instanceNames,
      int partitions, int replicas, String dbName) throws Exception
  {
    if (instanceNames.size() <= replicas)
    {
      throw new IllegalArgumentException(
          "Replicas must be less than number of storage nodes");
    }

    int numberOfClusters = 1;
    List<List<String>> nodesInClusters = new ArrayList<List<String>>();
    nodesInClusters.add(instanceNames);
    List<Integer> clusterWeights = new ArrayList<Integer>();
    clusterWeights.add(1);

    HashMap<String, Object> rushConfig = buildRushConfig(numberOfClusters,
        nodesInClusters, replicas, clusterWeights);
    RUSHrHash rushHash = new RUSHrHash(rushConfig);

    ZNRecord result = new ZNRecord();
    result.setId(dbName);

    for (int i = 0; i < partitions; i++)
    {
      int partitionId = i;
      String partitionName = dbName + ".partition-" + partitionId;

      ArrayList<HashMap> partitionAssignmentResult = rushHash
          .findNode(partitionName);
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
      // the first in the list is the node that contains the master
      partitionAssignment.put(nodeNames.get(0), "MASTER");

      // for the jth replica, we put it on (masterNode + j) % nodes-th
      // node
      for (int j = 1; j < nodeNames.size(); j++)
      {
        partitionAssignment.put(nodeNames.get(j), "SLAVE");
      }

      result.setMapField(partitionName, partitionAssignment);
    }
    result.setSimpleField("partitions", String.valueOf(partitions));
    return result;
  }

  /**
   * Helper function to see how many partitions are mapped to different
   * instances in two ideal states
   * */
  public static void printDiff(ZNRecord record1, ZNRecord record2)
  {
    int diffCount = 0;
    for (String key : record1.getMapFields().keySet())
    {
      Map<String, String> map1 = record1.getMapField(key);
      Map<String, String> map2 = record2.getMapField(key);

      for (String k : map1.keySet())
      {
        if (!map2.containsKey(k))
        {
          diffCount++;
        } else if (!map1.get(k).equalsIgnoreCase(map2.get(k)))
        {
          diffCount++;
        }
      }
    }
    System.out.println("diff count = " + diffCount);
  }

  /**
   * Helper function to calculate and print the standard deviation of the
   * partition assignment ideal state.
   * */
  public static void printIdealStateStats(ZNRecord record)
  {
    Map<String, Integer> countsMap = new TreeMap<String, Integer>();
    for (String key : record.getMapFields().keySet())
    {
      Map<String, String> map1 = record.getMapField(key);
      for (String k : map1.keySet())
      {
        if (!countsMap.containsKey(k))
        {
          countsMap.put(k, new Integer(0));//
        }
        if (map1.get(k).equalsIgnoreCase("MASTER"))
        {
          countsMap.put(k, countsMap.get(k).intValue() + 1);
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
    System.out.println();
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

    System.out.println("Max count: " + maxCount + " min count:" + minCount);
    int steps = 10;
    int stepLen = (maxCount - minCount) / steps;
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
    int partitions = 6000, replicas = 4;
    String dbName = "espressoDB1";
    List<String> instanceNames = new ArrayList<String>();
    List<List<String>> instanceCluster1 = new ArrayList<List<String>>();
    for (int i = 0; i < 10; i++)
    {
      instanceNames.add("localhost_123" + i);
    }
    instanceCluster1.add(instanceNames);
    List<Integer> weights1 = new ArrayList<Integer>();
    weights1.add(1);
    ZNRecord result = IdealStateCalculatorByRush.calculateIdealState(
        instanceCluster1, weights1, partitions, replicas, dbName);

    printIdealStateStats(result);

    List<String> instanceNames2 = new ArrayList<String>();
    for (int i = 400; i < 410; i++)
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
