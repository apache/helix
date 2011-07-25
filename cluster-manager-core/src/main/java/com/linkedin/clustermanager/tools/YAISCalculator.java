package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class YAISCalculator
{
  static class Assignment
  {
    private final int numNodes;
    private final int replication;
    Partition[] partitions;
    Node[] nodes;

    public Assignment(int numNodes, int numPartitions, int replication)
    {
      this.numNodes = numNodes;
      this.replication = replication;
      partitions = new Partition[numPartitions];
      for (int i = 0; i < numPartitions; i++)
      {
        partitions[i] = new Partition(i, replication);
      }
      nodes = new Node[numNodes];
      for (int i = 0; i < numNodes; i++)
      {
        nodes[i] = new Node(replication);
      }
    }

    public void assign(int partitionId, int replicaId, int nodeId)
    {
      System.out.println("Assigning (" + partitionId + "," + replicaId
          + ") to " + nodeId);
      partitions[partitionId].nodeIds[replicaId] = nodeId;
      nodes[nodeId].partitionLists.get(replicaId).push(partitionId);
    }

    public void unassign(int partitionId, int replicaId)
    {

    }

    Integer[] getPartitionsPerNode(int nodeId, int replicaId)
    {
      List<Integer> partitionsList = new ArrayList<Integer>();
      for (Partition p : partitions)
      {
        if (p.nodeIds[replicaId] == nodeId)
        {
          partitionsList.add(p.partionId);
        }
      }
      Integer[] array = new Integer[partitionsList.size()];
      partitionsList.toArray(array);
      return array;
    }

    public void printPerNode()
    {
      for (int nodeId = 0; nodeId < numNodes; nodeId++)
      {
        for (int r = 0; r < replication; r++)
        {
          StringBuilder sb = new StringBuilder();
          sb.append("(").append(nodeId).append(",").append(r).append("):\t");
          Node node = nodes[nodeId];
          LinkedList<Integer> linkedList = node.partitionLists.get(r);
          for (int partitionId : linkedList)
          {
            sb.append(partitionId).append(",");
          }
          System.out.println(sb.toString());
        }

      }
    }
  }

  static class Partition
  {

    final int partionId;

    public Partition(int partionId, int replication)
    {
      this.partionId = partionId;
      nodeIds = new int[replication];
      Arrays.fill(nodeIds, -1);
    }

    int nodeIds[];
  }

  static class Node
  {
    private final int replication;
    ArrayList<LinkedList<Integer>> partitionLists;

    public Node(int replication)
    {
      this.replication = replication;
      partitionLists = new ArrayList<LinkedList<Integer>>(replication);
      for (int i = 0; i < replication; i++)
      {
        partitionLists.add(new LinkedList<Integer>());
      }
    }

  }

  public static void main(String[] args)
  {
    doAssignment(new int[]
    { 5 }, 120, 3);
  }

  private static void doAssignment(int[] nodes, int partitions, int replication)
  {
    int N = nodes[0];
    int totalNodes = 0;
    for (int temp : nodes)
    {
      totalNodes += temp;
    }
    Assignment assignment = new Assignment(totalNodes, partitions, replication);
    int nodeId = 0;
    for (int i = 0; i < partitions; i++)
    {
      assignment.assign(i, 0, nodeId);
      nodeId = (nodeId + 1) % N;
    }
    Random random = new Random();
    for (int r = 1; r < replication; r++)
    {
      for (int id = 0; id < N; id++)
      {
        Integer[] partitionsPerNode = assignment.getPartitionsPerNode(id, 0);
        boolean[] used = new boolean[partitionsPerNode.length];
        Arrays.fill(used, false);
        System.out.println(id + "-" + partitionsPerNode.length);
        nodeId = (id + r) % N;
        int count = partitionsPerNode.length;
        boolean done = false;
        do
        {
          if (nodeId != id)
          {
            int nextInt = random.nextInt(count);
            int temp = 0;
            for (int b = 0; b < used.length; b++)
            {
              if (!used[b] && temp == nextInt)
              {
                assignment.assign(partitionsPerNode[b], r, nodeId);
                used[b] = true;
                break;
              }
            }
          }
          nodeId = (nodeId + 1) % N;
        } while (count > 0);

      }
    }
    if (nodes.length > 1)
    {
      int prevNodeCount = nodes[0];
      for (int i = 1; i < nodes.length; i++)
      {
        int newNodeCount = prevNodeCount + nodes[i];
        int masterPartitionsToMove = (int) ((partitions * 1.0 / prevNodeCount - partitions
            * 1.0 / newNodeCount) * 1 * prevNodeCount);
        while (masterPartitionsToMove > 0)
        {

        }

      }
    }
    assignment.printPerNode();
  }

}
