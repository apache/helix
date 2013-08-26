package org.apache.helix.controller.strategy;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState.IdealStateProperty;

public class ConsistentHashingMasterSlaveStrategy {
  /**
   * Interface to calculate the hash function value of a string
   */
  public interface HashFunction {
    public int getHashValue(String key);
  }

  /**
   * The default string hash function. Same as the default function used by
   * Voldmort
   */
  public static class FnvHash implements HashFunction {
    private static final long FNV_BASIS = 0x811c9dc5;
    private static final long FNV_PRIME = (1 << 24) + 0x193;
    public static final long FNV_BASIS_64 = 0xCBF29CE484222325L;
    public static final long FNV_PRIME_64 = 1099511628211L;

    public int hash(byte[] key) {
      long hash = FNV_BASIS;
      for (int i = 0; i < key.length; i++) {
        hash ^= 0xFF & key[i];
        hash *= FNV_PRIME;
      }
      return (int) hash;
    }

    public long hash64(long val) {
      long hashval = FNV_BASIS_64;
      for (int i = 0; i < 8; i++) {
        long octet = val & 0x00ff;
        val = val >> 8;
        hashval = hashval ^ octet;
        hashval = hashval * FNV_PRIME_64;
      }
      return Math.abs(hashval);
    }

    @Override
    public int getHashValue(String key) {
      return hash(key.getBytes());
    }

  }

  /**
   * Calculate the ideal state for list of instances clusters using consistent
   * hashing.
   * @param instanceNames
   *          List of instance names.
   * @param partitions
   *          the partition number of the database
   * @param replicas
   *          the replication degree
   * @param resourceName
   *          the name of the database
   * @return The ZNRecord that contains the ideal state
   */
  public static ZNRecord calculateIdealState(List<String> instanceNames, int partitions,
      int replicas, String resourceName, HashFunction hashFunc) {
    return calculateIdealState(instanceNames, partitions, replicas, resourceName, hashFunc, 65536);
  }

  /**
   * Calculate the ideal state for list of instances clusters using consistent
   * hashing.
   * @param instanceNames
   *          List of instance names.
   * @param partitions
   *          the partition number of the database
   * @param replicas
   *          the replication degree
   * @param resourceName
   *          the name of the database
   * @param hashRingSize
   *          the size of the hash ring used by consistent hashing
   * @return The ZNRecord that contains the ideal state
   */
  public static ZNRecord calculateIdealState(List<String> instanceNames, int partitions,
      int replicas, String resourceName, HashFunction hashFunc, int hashRingSize) {
    ZNRecord result = new ZNRecord(resourceName);

    int[] hashRing = generateEvenHashRing(instanceNames, hashRingSize);
    result.setSimpleField(IdealStateProperty.NUM_PARTITIONS.toString(), String.valueOf(partitions));
    Random rand = new Random(0xc0ffee);
    for (int i = 0; i < partitions; i++) {
      String partitionName = resourceName + ".partition-" + i;
      int hashPos = rand.nextInt() % hashRingSize;
      // (int)(hashFunc.getHashValue(partitionName) % hashRingSize);
      hashPos = hashPos < 0 ? (hashPos + hashRingSize) : hashPos;
      // System.out.print(hashPos+ " ");
      // if(i % 120 == 0) System.out.println();
      Map<String, String> partitionAssignment = new TreeMap<String, String>();
      // the first in the list is the node that contains the master
      int masterPos = hashRing[hashPos];
      partitionAssignment.put(instanceNames.get(masterPos), "MASTER");

      // partitionAssignment.put("hash", "" + hashPos + " " + masterPos);

      // Put slaves in next has ring positions. We need to make sure that no
      // more than 2 slaves
      // are mapped to one node.
      for (int j = 1; j <= replicas; j++) {
        String next = instanceNames.get(hashRing[(hashPos + j) % hashRingSize]);
        while (partitionAssignment.containsKey(next)) {
          hashPos++;
          next = instanceNames.get(hashRing[(hashPos + j) % hashRingSize]);
        }
        partitionAssignment.put(next, "SLAVE");
      }
      result.setMapField(partitionName, partitionAssignment);
    }
    return result;
  }

  /**
   * Generate the has ring for consistent hashing.
   * @param instanceNames
   *          List of instance names.
   * @param hashRingSize
   *          the size of the hash ring used by consistent hashing
   * @return The int array as the hashing. it contains random values ranges from
   *         0..size of instanceNames-1
   */
  public static int[] generateHashRing(List<String> instanceNames, int hashRingSize) {
    int[] result = new int[hashRingSize];
    for (int i = 0; i < result.length; i++) {
      result[i] = 0;
    }
    int instances = instanceNames.size();
    // The following code generates the random distribution
    for (int i = 1; i < instances; i++) {
      putNodeOnHashring(result, i, hashRingSize / (i + 1), i);
    }
    return result;
  }

  public static int[] generateEvenHashRing(List<String> instanceNames, int hashRingSize) {
    int[] result = new int[hashRingSize];
    for (int i = 0; i < result.length; i++) {
      result[i] = 0;
    }
    int instances = instanceNames.size();
    // The following code generates the random distribution
    for (int i = 1; i < instances; i++) {
      putNodeEvenOnHashRing(result, i, i + 1);
    }
    return result;
  }

  private static void putNodeEvenOnHashRing(int[] hashRing, int nodeVal, int totalValues) {
    int newValNum = hashRing.length / totalValues;
    assert (newValNum > 0);
    Map<Integer, List<Integer>> valueIndex = buildValueIndex(hashRing);
    int nSources = valueIndex.size();
    int remainder = newValNum % nSources;

    List<List<Integer>> positionLists = new ArrayList<List<Integer>>();
    for (List<Integer> list : valueIndex.values()) {
      positionLists.add(list);
    }
    class ListComparator implements Comparator<List<Integer>> {
      @Override
      public int compare(List<Integer> o1, List<Integer> o2) {
        return (o1.size() > o2.size() ? -1 : (o1.size() == o2.size() ? 0 : 1));
      }
    }
    Collections.sort(positionLists, new ListComparator());

    for (List<Integer> oldValPositions : positionLists) {
      // List<Integer> oldValPositions = valueIndex.get(oldVal);
      int nValsToReplace = newValNum / nSources;
      assert (nValsToReplace > 0);
      if (remainder > 0) {
        nValsToReplace++;
        remainder--;
      }
      // System.out.print(oldValPositions.size()+" "+nValsToReplace+"  ");
      putNodeValueOnHashRing(hashRing, nodeVal, nValsToReplace, oldValPositions);
      // randomly take nValsToReplace positions in oldValPositions and make them
    }
    // System.out.println();
  }

  private static void putNodeValueOnHashRing(int[] hashRing, int nodeVal, int numberOfValues,
      List<Integer> positions) {
    Random rand = new Random(nodeVal);
    // initialize the index array
    int[] index = new int[positions.size()];
    for (int i = 0; i < index.length; i++) {
      index[i] = i;
    }

    int nodesLeft = index.length;

    for (int i = 0; i < numberOfValues; i++) {
      // Calculate a random index
      int randIndex = rand.nextInt() % nodesLeft;
      if (randIndex < 0) {
        randIndex += nodesLeft;
      }
      hashRing[positions.get(index[randIndex])] = nodeVal;

      // swap the random index and the last available index, and decrease the
      // nodes left
      int temp = index[randIndex];
      index[randIndex] = index[nodesLeft - 1];
      index[nodesLeft - 1] = temp;
      nodesLeft--;
    }
  }

  private static Map<Integer, List<Integer>> buildValueIndex(int[] hashRing) {
    Map<Integer, List<Integer>> result = new TreeMap<Integer, List<Integer>>();
    for (int i = 0; i < hashRing.length; i++) {
      if (!result.containsKey(hashRing[i])) {
        List<Integer> list = new ArrayList<Integer>();
        result.put(hashRing[i], list);
      }
      result.get(hashRing[i]).add(i);
    }
    return result;
  }

  /**
   * Uniformly put node values on the hash ring. Derived from the shuffling
   * algorithm
   * @param result
   *          the hash ring array.
   * @param nodeValue
   *          the int value to be added to the hash ring this time
   * @param numberOfNodes
   *          number of node values to put on the hash ring array
   * @param randomSeed
   *          the random seed
   */
  public static void putNodeOnHashring(int[] result, int nodeValue, int numberOfNodes,
      int randomSeed) {
    Random rand = new Random(randomSeed);
    // initialize the index array
    int[] index = new int[result.length];
    for (int i = 0; i < index.length; i++) {
      index[i] = i;
    }

    int nodesLeft = index.length;

    for (int i = 0; i < numberOfNodes; i++) {
      // Calculate a random index
      int randIndex = rand.nextInt() % nodesLeft;
      if (randIndex < 0) {
        randIndex += nodesLeft;
      }
      if (result[index[randIndex]] == nodeValue) {
        assert (false);
      }
      result[index[randIndex]] = nodeValue;

      // swap the random index and the last available index, and decrease the
      // nodes left
      int temp = index[randIndex];
      index[randIndex] = index[nodesLeft - 1];
      index[nodesLeft - 1] = temp;

      nodesLeft--;
    }
  }

  /**
   * Helper function to see how many partitions are mapped to different
   * instances in two ideal states
   */
  public static void printDiff(ZNRecord record1, ZNRecord record2) {
    int diffCount = 0;
    for (String key : record1.getMapFields().keySet()) {
      Map<String, String> map1 = record1.getMapField(key);
      Map<String, String> map2 = record2.getMapField(key);

      for (String k : map1.keySet()) {
        if (!map2.containsKey(k)) {
          diffCount++;
        } else if (!map1.get(k).equalsIgnoreCase(map2.get(k))) {
          diffCount++;
        }
      }
    }
    System.out.println("diff count = " + diffCount);
  }

  /**
   * Helper function to compare the difference between two hashing buffers
   */
  public static void compareHashrings(int[] ring1, int[] ring2) {
    int diff = 0;
    for (int i = 0; i < ring1.length; i++) {
      if (ring1[i] != ring2[i]) {
        diff++;
      }
    }
    System.out.println("ring diff: " + diff);
  }

  public static void printNodeOfflineOverhead(ZNRecord record) {
    // build node -> partition map
    Map<String, Set<String>> nodeNextMap = new TreeMap<String, Set<String>>();
    for (String partitionName : record.getMapFields().keySet()) {
      Map<String, String> map1 = record.getMapField(partitionName);
      String master = "", slave = "";
      for (String nodeName : map1.keySet()) {
        if (!nodeNextMap.containsKey(nodeName)) {
          nodeNextMap.put(nodeName, new TreeSet<String>());
        }

        // String master = "", slave = "";
        if (map1.get(nodeName).equalsIgnoreCase("MASTER")) {
          master = nodeName;
        } else {
          if (slave.equalsIgnoreCase("")) {
            slave = nodeName;
          }
        }

      }
      nodeNextMap.get(master).add(slave);
    }
    System.out.println("next count: ");
    for (String key : nodeNextMap.keySet()) {
      System.out.println(nodeNextMap.get(key).size() + " ");
    }
    System.out.println();
  }

  /**
   * Helper function to calculate and print the standard deviation of the
   * partition assignment ideal state, also the min/max of master partitions
   * that is hosted on each node
   */
  public static void printIdealStateStats(ZNRecord record, String value) {
    Map<String, Integer> countsMap = new TreeMap<String, Integer>();
    for (String key : record.getMapFields().keySet()) {
      Map<String, String> map1 = record.getMapField(key);
      for (String k : map1.keySet()) {
        if (!countsMap.containsKey(k)) {
          countsMap.put(k, new Integer(0));//
        }
        if (value.equals("") || map1.get(k).equalsIgnoreCase(value)) {
          countsMap.put(k, countsMap.get(k).intValue() + 1);
        }
      }
    }
    double sum = 0;
    int maxCount = 0;
    int minCount = Integer.MAX_VALUE;

    System.out.println("Partition distributions: ");
    for (String k : countsMap.keySet()) {
      int count = countsMap.get(k);
      sum += count;
      if (maxCount < count) {
        maxCount = count;
      }
      if (minCount > count) {
        minCount = count;
      }
      System.out.print(count + " ");
    }
    System.out.println();
    double mean = sum / (countsMap.size());
    // calculate the deviation of the node distribution
    double deviation = 0;
    for (String k : countsMap.keySet()) {
      double count = countsMap.get(k);
      deviation += (count - mean) * (count - mean);
    }
    System.out.println("Mean: " + mean + " normal deviation:"
        + Math.sqrt(deviation / countsMap.size()));

    System.out.println("Max count: " + maxCount + " min count:" + minCount);
    /*
     * int steps = 10; int stepLen = (maxCount - minCount)/steps; List<Integer>
     * histogram = new ArrayList<Integer>((maxCount - minCount)/stepLen + 1);
     * for(int i = 0; i< (maxCount - minCount)/stepLen + 1; i++) {
     * histogram.add(0); } for(String k :countsMap.keySet()) { int count =
     * countsMap.get(k); int stepNo = (count - minCount)/stepLen;
     * histogram.set(stepNo, histogram.get(stepNo) +1); }
     * System.out.println("histogram:"); for(Integer x : histogram) {
     * System.out.print(x+" "); }
     */
  }

  public static void printHashRingStat(int[] hashRing) {
    double sum = 0, mean = 0, deviation = 0;
    Map<Integer, Integer> countsMap = new TreeMap<Integer, Integer>();
    for (int i = 0; i < hashRing.length; i++) {
      if (!countsMap.containsKey(hashRing[i])) {
        countsMap.put(hashRing[i], new Integer(0));//
      }
      countsMap.put(hashRing[i], countsMap.get(hashRing[i]).intValue() + 1);
    }
    int maxCount = Integer.MIN_VALUE;
    int minCount = Integer.MAX_VALUE;
    for (Integer k : countsMap.keySet()) {
      int count = countsMap.get(k);
      sum += count;
      if (maxCount < count) {
        maxCount = count;
      }
      if (minCount > count) {
        minCount = count;
      }
    }
    mean = sum / countsMap.size();
    for (Integer k : countsMap.keySet()) {
      int count = countsMap.get(k);
      deviation += (count - mean) * (count - mean);
    }
    System.out.println("hashring Mean: " + mean + " normal deviation:"
        + Math.sqrt(deviation / countsMap.size()));

  }

  static int[] getFnvHashArray(List<String> strings) {
    int[] result = new int[strings.size()];
    int i = 0;
    ConsistentHashingMasterSlaveStrategy.FnvHash hashfunc =
        new ConsistentHashingMasterSlaveStrategy.FnvHash();
    for (String s : strings) {
      int val = hashfunc.getHashValue(s) % 65536;
      if (val < 0)
        val += 65536;
      result[i++] = val;
    }
    return result;
  }

  static void printArrayStat(int[] vals) {
    double sum = 0, mean = 0, deviation = 0;

    for (int i = 0; i < vals.length; i++) {
      sum += vals[i];
    }
    mean = sum / vals.length;
    for (int i = 0; i < vals.length; i++) {
      deviation += (mean - vals[i]) * (mean - vals[i]);
    }
    System.out.println("normalized deviation: " + Math.sqrt(deviation / vals.length) / mean);
  }

  public static void main(String args[]) throws Exception {
    // Test the hash ring generation
    List<String> instanceNames = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      instanceNames.add("localhost_123" + i);
    }

    // int[] ring1 =
    // IdealCalculatorByConsistentHashing.generateEvenHashRing(instanceNames,
    // 65535);
    // printHashRingStat(ring1);
    // int[] ring1 = getFnvHashArray(instanceNames);
    // printArrayStat(ring1);

    int partitions = 200, replicas = 2;
    String dbName = "espressoDB1";

    ZNRecord result =
        ConsistentHashingMasterSlaveStrategy.calculateIdealState(instanceNames, partitions,
            replicas, dbName, new ConsistentHashingMasterSlaveStrategy.FnvHash());
    System.out.println("\nMaster :");
    printIdealStateStats(result, "MASTER");

    System.out.println("\nSlave :");
    printIdealStateStats(result, "SLAVE");

    System.out.println("\nTotal :");
    printIdealStateStats(result, "");

    printNodeOfflineOverhead(result);
    /*
     * ZNRecordSerializer serializer = new ZNRecordSerializer(); byte[] bytes;
     * bytes = serializer.serialize(result); // System.out.println(new
     * String(bytes));
     * List<String> instanceNames2 = new ArrayList<String>(); for(int i = 0;i <
     * 40; i++) { instanceNames2.add("localhost_123"+i); }
     * ZNRecord result2 =
     * IdealCalculatorByConsistentHashing.calculateIdealState( instanceNames2,
     * partitions, replicas, dbName, new
     * IdealCalculatorByConsistentHashing.FnvHash());
     * printDiff(result, result2);
     * //IdealCalculatorByConsistentHashing.printIdealStateStats(result2);
     * int[] ring2 =
     * IdealCalculatorByConsistentHashing.generateHashRing(instanceNames2,
     * 30000);
     * IdealCalculatorByConsistentHashing.compareHashrings(ring1, ring2);
     * //printNodeStats(result); //printNodeStats(result2); bytes =
     * serializer.serialize(result2); printHashRingStat(ring2); //
     * System.out.println(new String(bytes));
     */

  }
}
