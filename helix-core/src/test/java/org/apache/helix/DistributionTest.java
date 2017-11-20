package org.apache.helix;
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

import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.HelixUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class DistributionTest {
  private static String instanceFolderPath;
  private static String instanceList;
  private static String idealStateFolderPath;
  private static String idealStateList;

  String Path = "/home/jjwang/Desktop/FEAP-test";
  //String Path = "/Users/jjwang/Desktop/FEAP-test";

  @DataProvider(name = "rebalanceStrategies")
  public static String[][] rebalanceStrategies() {
    return new String[][] {
        //{AutoRebalanceStrategy.class.getName()},
        { CrushRebalanceStrategy.class.getName() },
        //{ MultiRoundCrushRebalanceStrategy.class.getName() },
        //{ CrushEdRebalanceStrategy.class.getName() }
    };
  }

  String[] fabrics = { "lor1", "lva1", "ltx1", "lsg1",
  };
  String[] clusters = { "ESPRESSO_IDENTITY", "ESPRESSO_MT-MD-1", "ESPRESSO_TSCP", "ESPRESSO_MT_PHASE1",
          "ESPRESSO_MT-MD-3", "ESPRESSO_USCP", "ESPRESSO_MT-LEGACY", /* "venice-0" */
  };
  String topState = "master";
  float[] nodeAdjustSimulator =
      { /*-0.5f, -0.2f, -0.1f, -0.01f, */ 0.01f, 0.1f, 0.2f, 0.5f, 1f};

  @Test(dataProvider = "rebalanceStrategies")
  public void testNodeChange(String rebalanceStrategyClass) throws Exception {
    for (String cluster : clusters) {
      System.out.println(cluster
          + "\tChangeType\tNumOfNodeChange\tDiffRate\tTotalMv\tTotalMvRate\tExtraMvRate\tExtraMvRateComparedWithAvgDist\tTopStateChange\tTopStateChangeRate\tTopStateChangeWithNewDeployRate\tExtraTopStateChangeRate");
      for (String fabric : fabrics) {
        String path = Path + "/" + cluster + "/" + fabric;
        if (new File(path).exists()) {
          System.out.print(fabric);
          for (float adjustRate : nodeAdjustSimulator) {
            Set<String> deltaNode = new HashSet<>();
            List<String> liveInstances = new ArrayList<>();
            Map<String, Map<String, String>> resultA =
                calculate(path, rebalanceStrategyClass, adjustRate, deltaNode, liveInstances);
            double[] distEval = checkEvenness(liveInstances, resultA, false);
            if (adjustRate != 0) {
              Map<String, Map<String, String>> result =
                  calculate(path, rebalanceStrategyClass, 0, deltaNode, new ArrayList<String>());
              double[] diff = checkMovement(result, resultA, deltaNode, false);
              System.out.println(
                  "\t" + (adjustRate > 0 ? "Adding\t" : "Disabling\t") + diff[0] + "\t"
                      + distEval[3] + "\t" + diff[1] + "\t" + diff[2] + "\t" + diff[3] + "\t"
                      + diff[8] + "\t" + diff[4] + "\t" + diff[5] + "\t" + diff[10] + "\t"
                      + diff[6]);
            }
          }
        }
      }
      System.out.println();
    }
  }

  @Test(dataProvider = "rebalanceStrategies")
  public void testDist(String rebalanceStrategyClass) throws Exception {
    for (String cluster : clusters) {
      System.out.println(cluster
          + "\tTotalReplica\tMinReplica\tMaxReplica\tDiffRate\tSTDEV\tMinTopState\tMaxTopState\ttopStateDiffRate\ttopStateSTDEV");
      for (String fabric : fabrics) {
        String path = Path + "/" + cluster + "/" + fabric;
        if (new File(path).exists()) {
          Set<String> deltaNode = new HashSet<>();
          List<String> liveInstances = new ArrayList<>();
          Map<String, Map<String, String>> result =
              calculate(path, rebalanceStrategyClass, 0, deltaNode, liveInstances);
          double[] distEval = checkEvenness(liveInstances, result, false);
          System.out.println(
              fabric + "\t" + distEval[0] + "\t" + distEval[1] + "\t" + distEval[2] + "\t"
                  + distEval[3] + "\t" + distEval[4] + "\t" + distEval[5] + "\t" + distEval[6]
                  + "\t" + distEval[7] + "\t" + distEval[8]);
        }
      }
      System.out.println();
    }
  }

  int _replica = 1;
  int partitionCount = 101;
  int faultZone = 10;
  int[] resourceCounts = new int[] { 100 };
  int[] nodeCounts = new int[] { 100, /*100, 200, 500, 1000*/ };

  @Test(dataProvider = "rebalanceStrategies")
  public void testDistUsingRandomTopo(String rebalanceStrategyClass) throws Exception {
    for (int nodeCount : nodeCounts) {
      for (int resourceCount : resourceCounts) {
        System.out.println(
            "NodeCount\tResourceCount\tTotalReplica\tMinReplica\tMaxReplica\tDiffRate\tSTDEV\tMinTopState\tMaxTopState\tTopStateDiffRate\tTopStateSTDEV");
        List<String> liveInstances = new ArrayList<>();
        Map<String, Map<String, String>> result =
            calculateUsingRandomTopo(rebalanceStrategyClass, _replica, partitionCount,
                resourceCount, nodeCount, faultZone, liveInstances);
        double[] distEval = checkEvenness(liveInstances, result, false);
        System.out.println(
            nodeCount + "\t" + resourceCount + "\t" + distEval[0] + "\t" + distEval[1] + "\t"
                + distEval[2] + "\t" + distEval[3] + "\t" + distEval[4] + "\t" + distEval[5] + "\t"
                + distEval[6] + "\t" + distEval[7] + "\t" + distEval[8]);
      }
    }

    System.out.println();
  }

  @Test(dataProvider = "rebalanceStrategies")
  public void testRollingUpgrade(String rebalanceStrategyClass) throws Exception {
    for (String cluster : clusters) {
      System.out.println(cluster
          + "\tTotalMv\tTotalMvRate\tExtraMvRate\tExtraMvRateComparedWithAvgDist\tTopStateChange\tTopStateChangeRate\tTopStateChangeWithNewDeployRate\tExtraTopStateChange");
      for (String fabric : fabrics) {
        String path = Path + "/" + cluster + "/" + fabric;
        if (new File(path).exists()) {

          List<List<String>> deltaNodesHistory = new ArrayList<>();
          List<List<String>> liveInstancesHistory = new ArrayList<>();

          List<Map<String, Map<String, String>>> mappingHistory =
              calculateRollingUpgrade(path, rebalanceStrategyClass, deltaNodesHistory,
                  liveInstancesHistory, true);

          Map<String, Map<String, String>> basicMapping =
              calculate(path, rebalanceStrategyClass, 0, new HashSet<String>(),
                  new ArrayList<String>());

          double[] maxDiff = new double[8];
          for (int i = 0; i < mappingHistory.size(); i++) {
            List<String> deltaNode = deltaNodesHistory.get(i);
            Map<String, Map<String, String>> mapA = mappingHistory.get(i);

            Map<String, Map<String, String>> mapB = basicMapping;
            if (i != 0) {
              deltaNode.addAll(deltaNodesHistory.get(i - 1));
              mapB = mappingHistory.get(i - 1);
            }
            double[] diff = checkMovement(mapB, mapA, deltaNode, false);

            maxDiff[0] = Math.max(diff[1], maxDiff[0]);
            maxDiff[1] = Math.max(diff[2], maxDiff[1]);
            maxDiff[2] = Math.max(diff[3], maxDiff[2]);
            maxDiff[3] = Math.max(diff[4], maxDiff[3]);
            maxDiff[4] = Math.max(diff[5], maxDiff[4]);
            maxDiff[5] = Math.max(diff[6], maxDiff[5]);
            maxDiff[6] = Math.max(diff[8], maxDiff[6]);
            maxDiff[7] = Math.max(diff[10], maxDiff[7]);
          }
          System.out.println(
              fabric + "\t" + maxDiff[0] + "\t" + maxDiff[1] + "\t" + maxDiff[2] + "\t" + maxDiff[6]
                  + "\t" + maxDiff[3] + "\t" + maxDiff[4] + "\t" + maxDiff[7] + "\t" + maxDiff[5]);
        }
      }
      System.out.println();
    }
  }

  public List<Map<String, Map<String, String>>> calculateRollingUpgrade(String Path,
      String rebalanceStrategyClass, List<List<String>> deltaNodesHistory,
      List<List<String>> liveInstancesHistory, boolean recoverNode) throws Exception {
    instanceFolderPath = Path + "/instanceConfigs/";
    instanceList = Path + "/instance";
    idealStateFolderPath = Path + "/idealStates/";
    idealStateList = Path + "/idealstate";
    Path path = Paths.get(Path + "/clusterConfig");
    ZNRecord record = (ZNRecord) new ZNRecordSerializer().deserialize(Files.readAllBytes(path));
    ClusterConfig clusterConfig = new ClusterConfig(record);
    List<String> allNodes = new ArrayList<>();
    List<InstanceConfig> instanceConfigs =
        getInstanceConfigs(instanceFolderPath, instanceList, allNodes);
    List<IdealState> idealStates = getIdealStates(idealStateFolderPath, idealStateList);

    List<String> deltaNodes = new ArrayList<>();

    List<Map<String, Map<String, String>>> totalMapHistory = new ArrayList<>();
    for (String downNode : allNodes) {
      deltaNodes.add(downNode);

      List<String> liveInstances = new ArrayList<>(allNodes);
      liveInstances.removeAll(deltaNodes);
      Map<String, Map<String, String>> totalMaps = new HashMap<>();

      totalMapHistory.add(totalMaps);
      liveInstancesHistory.add(liveInstances);
      deltaNodesHistory.add(new ArrayList<>(deltaNodes));

      Map<String, Integer> partitions = new HashMap<>();
      for (int i = 0; i < idealStates.size(); i++) {
        Map<String, Map<String, String>> maps = HelixUtil
            .getIdealAssignmentForFullAuto(clusterConfig, instanceConfigs, liveInstances,
                idealStates.get(i), new ArrayList<>(idealStates.get(i).getPartitionSet()),
                rebalanceStrategyClass);
        for (String partitionName : idealStates.get(i).getPartitionSet()) {
          partitions.put(partitionName, idealStates.get(i).getReplicaCount(liveInstances.size()));
        }
        totalMaps.putAll(maps);
      }
      Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
      for (InstanceConfig config : instanceConfigs) {
        instanceConfigMap.put(config.getInstanceName(), config);
      }
      verifyDistribution(totalMaps, liveInstances, partitions,
          new Topology(new ArrayList<>(instanceConfigMap.keySet()), liveInstances,
              instanceConfigMap, clusterConfig));
      if (recoverNode) {
        deltaNodes.remove(downNode);
      }
    }
    return totalMapHistory;
  }

  public Map<String, Map<String, String>> calculateUsingRandomTopo(String rebalanceStrategyClass,
      int replica, int partitionCount, int nodeCount, int resourceCount, int faultZone,
      List<String> liveInstances) throws Exception {
    String[] className = rebalanceStrategyClass.split("\\.");
    String PARTICIPANT_PREFIX =
        className[className.length - 1] + "_node_" + nodeCount + resourceCount;
    String RESOURCE_PREFIX =
        className[className.length - 1] + "_resource_" + nodeCount + resourceCount;
    String CLUSTER_NAME =
        className[className.length - 1] + nodeCount + resourceCount + "TestingCluster";

    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setFaultZoneType("zone");
    clusterConfig.setTopology("/zone/rack/instance");

    List<InstanceConfig> newInstanceConfigs = new ArrayList<>();
    Random rand = new Random();
    for (int i = 0; i < nodeCount; i++) {
      String nodeName = PARTICIPANT_PREFIX + Math.abs(rand.nextInt()) + "_" + i;
      String zone = "zone-" + i % faultZone;
      InstanceConfig newConfig = new InstanceConfig(nodeName);
      liveInstances.add(nodeName);
      newConfig.setInstanceEnabled(true);
      newConfig.setHostName(nodeName);
      newConfig.setPort(new Integer(i).toString());
      newConfig.setDomain(String
          .format("cluster=%s,zone=%s,rack=myRack,instance=%s", CLUSTER_NAME, zone, nodeName));
      newConfig.setWeight(1000);
      newConfig.setDelayRebalanceEnabled(false);
      newConfig.setMaxConcurrentTask(1000);
      newInstanceConfigs.add(newConfig);
    }

    Map<String, Map<String, String>> totalMaps = new HashMap<>();
    Map<String, Integer> partitions = new HashMap<>();
    List<IdealState> idealStates = new ArrayList<>();

    for (int i = 0; i < resourceCount; i++) {
      String resourceName = RESOURCE_PREFIX + "_" + i;
      IdealState idealState = new IdealState(resourceName);
      idealState.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setReplicas(new Integer(replica).toString());
      idealState.setNumPartitions(partitionCount);
      idealState.setRebalancerClassName(rebalanceStrategyClass);
      for (int p = 0; p < partitionCount; p++) {
        String partitionName = resourceName + "_" + p;
        idealState.setPreferenceList(partitionName, new ArrayList<String>());
      }
      idealStates.add(idealState);
    }

    long duration = 0;
    for (IdealState idealState : idealStates) {
      long startTime = System.currentTimeMillis();
      Map<String, Map<String, String>> maps = HelixUtil
          .getIdealAssignmentForFullAuto(clusterConfig, newInstanceConfigs, liveInstances,
              idealState, new ArrayList<>(idealState.getPartitionSet()), rebalanceStrategyClass);
      duration += System.currentTimeMillis() - startTime;

      for (String partitionName : idealState.getPartitionSet()) {
        partitions.put(partitionName, idealState.getReplicaCount(liveInstances.size()));
      }
      totalMaps.putAll(maps);
    }

    //System.out.println("Total running time:\t" + duration);

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    for (InstanceConfig config : newInstanceConfigs) {
      instanceConfigMap.put(config.getInstanceName(), config);
    }
    verifyDistribution(totalMaps, liveInstances, partitions,
        new Topology(new ArrayList<>(instanceConfigMap.keySet()), liveInstances, instanceConfigMap,
            clusterConfig));
    return totalMaps;
  }

  public Map<String, Map<String, String>> calculate(String Path, String rebalanceStrategyClass,
      float instanceAdjustRate, Set<String> deltaNode, List<String> liveInstances)
      throws Exception {
    instanceFolderPath = Path + "/instanceConfigs/";
    instanceList = Path + "/instance";
    idealStateFolderPath = Path + "/idealStates/";
    idealStateList = Path + "/idealstate";
    Path path = Paths.get(Path + "/clusterConfig");
    ZNRecord record = (ZNRecord) new ZNRecordSerializer().deserialize(Files.readAllBytes(path));
    ClusterConfig clusterConfig = new ClusterConfig(record);
    List<InstanceConfig> instanceConfigs =
        getInstanceConfigs(instanceFolderPath, instanceList, liveInstances);

    int adjustNodeCount = (int) (instanceAdjustRate > 0 ?
        Math.ceil(instanceAdjustRate * liveInstances.size()) :
        Math.floor(instanceAdjustRate * liveInstances.size()));

    if (adjustNodeCount > 0) {
      for (int i = 0; i < adjustNodeCount; i++) {
        int cloneIndex = i % (liveInstances.size() - 1);
        String nodeName = instanceConfigs.get(cloneIndex).getInstanceName() + "_random" + i;
        liveInstances.add(nodeName);
        InstanceConfig cloneConfig = new InstanceConfig(nodeName);
        cloneConfig.setHostName(nodeName);
        cloneConfig.setInstanceEnabled(true);
        cloneConfig.setPort(instanceConfigs.get(cloneIndex).getPort());
        cloneConfig.setDomain(instanceConfigs.get(cloneIndex).getDomain() + "_random" + i);
        if (instanceConfigs.get(cloneIndex).getWeight() > 0) {
          cloneConfig.setWeight(instanceConfigs.get(cloneIndex).getWeight());
        }
        cloneConfig
            .setDelayRebalanceEnabled(instanceConfigs.get(cloneIndex).isDelayRebalanceEnabled());
        if (instanceConfigs.get(cloneIndex).getMaxConcurrentTask() > 0) {
          cloneConfig.setMaxConcurrentTask(instanceConfigs.get(cloneIndex).getMaxConcurrentTask());
        }
        instanceConfigs.add(cloneConfig);
        deltaNode.add(nodeName);
      }
    } else {
      if (adjustNodeCount > liveInstances.size()) {
        throw new Exception("All nodes are removed, no assignment possible.");
      }
      for (int i = 0; i < Math.abs(adjustNodeCount); i++) {
        String nodeName = liveInstances.remove(i);
        deltaNode.add(nodeName);
      }
    }

    List<IdealState> idealStates = getIdealStates(idealStateFolderPath, idealStateList);
    Map<String, Map<String, String>> totalMaps = new HashMap<>();
    Map<String, Integer> partitions = new HashMap<>();

    long duration = 0;
    for (int i = 0; i < idealStates.size(); i++) {
      long startTime = System.currentTimeMillis();
      int partitionCount = idealStates.get(i).getNumPartitions();
      List<String> partitionList =
          new ArrayList<>(idealStates.get(i).getPartitionSet()).subList(0, partitionCount);
      Map<String, Map<String, String>> maps = HelixUtil
          .getIdealAssignmentForFullAuto(clusterConfig, instanceConfigs, liveInstances,
              idealStates.get(i), partitionList, rebalanceStrategyClass);
      for (String partitionName : partitionList) {
        partitions.put(partitionName, idealStates.get(i).getReplicaCount(liveInstances.size()));
      }
      duration += System.currentTimeMillis() - startTime;

      // print resource details
/*      Map<String, Set<String>> nodeMapping = convertMapping(maps);
      String partitionCountsStr = idealStates.get(i).getResourceName();
      List<String> sortedInstances = new ArrayList<>(liveInstances);
      Collections.sort(sortedInstances);
      for (String node : sortedInstances) {
        partitionCountsStr += "\t" + (nodeMapping.containsKey(node) ? nodeMapping.get(node).size() : 0);
      }
      System.out.println(partitionCountsStr);*/

      totalMaps.putAll(maps);
    }
    //System.out.println("Takes " + duration + "ms");
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    for (InstanceConfig config : instanceConfigs) {
      instanceConfigMap.put(config.getInstanceName(), config);
    }
    verifyDistribution(totalMaps, liveInstances, partitions,
        new Topology(new ArrayList<>(instanceConfigMap.keySet()), liveInstances, instanceConfigMap,
            clusterConfig));
    return totalMaps;
  }

  private void verifyDistribution(Map<String, Map<String, String>> map, List<String> liveInstances,
      Map<String, Integer> partitionExp, Topology topology) throws Exception {
    Map<String, Set<String>> faultZonePartition = new HashMap<>();
    Map<String, String> instanceFaultZone = new HashMap<>();
    for (Node node : topology.getFaultZones()) {
      faultZonePartition.put(node.getName(), new HashSet<String>());
      for (Node instance : Topology.getAllLeafNodes(node)) {
        instanceFaultZone.put(instance.getName(), node.getName());
      }
    }
    for (String partition : map.keySet()) {
      // no partition missing, no partition duplicate
      if (!partitionExp.containsKey(partition) || map.get(partition).size() != partitionExp
          .get(partition)) {
        throw new Exception("partition replica in mapping is not as expected");
      }
      partitionExp.remove(partition);
      // no partition on non-live node
      for (String instance : map.get(partition).keySet()) {
        if (!liveInstances.contains(instance)) {
          throw new Exception("assignment is not on a live node!");
        }
        // no fault zone conflict
        String faultZone = instanceFaultZone.get(instance);
        if (faultZonePartition.get(faultZone).contains(partition)) {
          throw new Exception("faultzone conflict!");
        }
        faultZonePartition.get(faultZone).add(partition);
      }
    }
    if (!partitionExp.isEmpty()) {
      throw new Exception("partition is not assigned");
    }
  }

  private double[] checkEvenness(List<String> liveInstances,
      Map<String, Map<String, String>> totalMaps, boolean verbose) {
    StringBuilder output = new StringBuilder();
    Map<String, List<String>> detailMap = new HashMap<>();
    Map<String, Integer> distributionMap = new TreeMap<>();
    Map<String, Integer> topStateDistributionMap = new HashMap<>();
    for (String instance : liveInstances) {
      distributionMap.put(instance, 0);
      topStateDistributionMap.put(instance, 0);
      detailMap.put(instance, new ArrayList<String>());
    }

    for (String partition : totalMaps.keySet()) {
      Map<String, String> instanceMap = totalMaps.get(partition);
      for (String instance : instanceMap.keySet()) {
        detailMap.get(instance).add(partition + "-" + totalMaps.get(partition).get(instance));
        distributionMap.put(instance, distributionMap.get(instance) + 1);
        if (instanceMap.get(instance).equalsIgnoreCase(topState)) {
          topStateDistributionMap.put(instance, topStateDistributionMap.get(instance) + 1);
        }
      }
    }

    int totalReplicas = 0;
    int minR = Integer.MAX_VALUE;
    int maxR = 0;
    int mminR = Integer.MAX_VALUE;
    int mmaxR = 0;
    for (String instance : distributionMap.keySet()) {
      output.append(instance + "\t" + distributionMap.get(instance) + "\tpartitions\t"
          + topStateDistributionMap.get(instance) + "\ttopStates\n");
      //output.append(instance + "\t:\t" + distributionMap.get(instance) + "\tpartitions\t" + topStateDistributionMap.get(instance) + "\ttopStates\t" + detailMap.get(instance) + "\n");
      totalReplicas += distributionMap.get(instance);
      minR = Math.min(minR, distributionMap.get(instance));
      maxR = Math.max(maxR, distributionMap.get(instance));
    }
    for (String instance : topStateDistributionMap.keySet()) {
      mminR = Math.min(mminR, topStateDistributionMap.get(instance));
      mmaxR = Math.max(mmaxR, topStateDistributionMap.get(instance));
    }

    output.append("Maximum holds " + maxR + " replicas and minimum holds " + minR
        + " replicas, differentiation is " + (double) (maxR - minR) / maxR * 100 + "%\n");
    output.append("Maximum holds " + mmaxR + " topStates and minimum holds " + mminR
        + " topStates, differentiation is " + (double) (mmaxR - mminR) / mmaxR * 100 + "%\n ");

    if (verbose) {
      System.out.println(output.toString());
    }
    return new double[] { totalReplicas, minR, maxR, (double) (maxR - minR) / maxR * 100,
        STDEV(new ArrayList<>(distributionMap.values())), mminR, mmaxR,
        (double) (mmaxR - mminR) / mmaxR * 100,
        STDEV(new ArrayList<>(topStateDistributionMap.values()))
    };
  }

  private double STDEV(List<Integer> data) {
    if (data.isEmpty() || data.size() == 1) {
      return 0;
    }
    double totalDiff = 0;
    double average = 0;
    for (int num : data) {
      average += num;
    }
    average /= data.size();
    for (int i = 0; i < data.size(); i++) {
      totalDiff += Math.pow(data.get(i) - average, 2);
    }
    return Math.sqrt(totalDiff) / (data.size() - 1);
  }

  private static List<InstanceConfig> getInstanceConfigs(String instanceFolderPath,
      String instanceList, List<String> liveInstances) throws IOException {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    for (ZNRecord record : getRecords(instanceFolderPath, instanceList)) {
      instanceConfigs.add(new InstanceConfig(record));
      liveInstances.add(record.getId());
    }
    return instanceConfigs;
  }

  private static List<IdealState> getIdealStates(String idealStateFolderPath, String idealStateList)
      throws IOException {
    List<IdealState> idealStates = new ArrayList<>();
    for (ZNRecord record : getRecords(idealStateFolderPath, idealStateList)) {
      IdealState idealState = new IdealState(record);
      try {
        BuiltInStateModelDefinitions.valueOf(idealState.getStateModelDefRef());
      } catch (IllegalArgumentException ex) {
        idealState.setStateModelDefRef("OnlineOffline");
      }
      idealStates.add(idealState);
    }
    return idealStates;
  }

  private static List<ZNRecord> getRecords(String folderPath, String list) throws IOException {
    List<ZNRecord> records = new ArrayList<>();
    List<String> names = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(list))) {
      String sCurrentLine = br.readLine();
      names.addAll(Arrays.asList(sCurrentLine.split(" ")));
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (String name : names) {
      Path path = Paths.get(folderPath + name);
      ZNRecord record = (ZNRecord) new ZNRecordSerializer().deserialize(Files.readAllBytes(path));
      records.add(record);
    }
    return records;
  }

  private Map<String, Map<String, Integer>> getStateCount(Map<String, Map<String, String>> map) {
    Map<String, Map<String, Integer>> mapStateCount = new HashMap<>();
    for (String partition : map.keySet()) {
      Map<String, Integer> stateCount = new HashMap<>();
      mapStateCount.put(partition, stateCount);
      for (String node : map.get(partition).keySet()) {
        String state = map.get(partition).get(node);
        if (!stateCount.containsKey(state)) {
          stateCount.put(state, 1);
        } else {
          stateCount.put(state, stateCount.get(state) + 1);
        }
      }
    }
    return mapStateCount;
  }

  private void verifyMaps(Map<String, Map<String, String>> map,
      Map<String, Map<String, String>> newMap) throws Exception {
    // check no partition lose
    Map<String, Map<String, Integer>> mapStateCount = getStateCount(map);
    Map<String, Map<String, Integer>> newMapStateCount = getStateCount(newMap);
    for (String partition : mapStateCount.keySet()) {
      if (!newMapStateCount.containsKey(partition)) {
        throw new Exception("mapping does not match");
      }
      for (String state : mapStateCount.get(partition).keySet()) {
        if (!newMapStateCount.get(partition).containsKey(state)
            || mapStateCount.get(partition).get(state) != newMapStateCount.get(partition)
            .get(state)) {
          throw new Exception("state does not match");
        }
      }
      for (String state : newMapStateCount.get(partition).keySet()) {
        if (!mapStateCount.get(partition).containsKey(state)) {
          throw new Exception("state does not match");
        }
      }
    }
    for (String partition : newMapStateCount.keySet()) {
      if (!mapStateCount.containsKey(partition)) {
        throw new Exception("mapping does not match");
      }
    }
  }

  private double[] checkMovement(Map<String, Map<String, String>> map,
      Map<String, Map<String, String>> newMap, Collection<String> deltaNodes, boolean verbose)
      throws Exception {
    verifyMaps(map, newMap);
    int totalChange = 0;
    int totalTopStateChange = 0;
    int totalTopStateChangeWithNewDeployment = 0;
    int totalPartition = 0;
    int totalTopState = 0;

    for (String partition : map.keySet()) {
      Map<String, String> origStates = map.get(partition);
      Map<String, String> newStates = newMap.get(partition);
      String topStateNode = "", newtopStateNode = "";
      for (String node : origStates.keySet()) {
        if (origStates.get(node).equalsIgnoreCase(topState)) {
          topStateNode = node;
        }
      }
      for (String node : newStates.keySet()) {
        if (newStates.get(node).equalsIgnoreCase(topState)) {
          newtopStateNode = node;
          totalTopState++;
        }
      }
      if (!topStateNode.equalsIgnoreCase(newtopStateNode)) {
        totalTopStateChange++;
        if (!origStates.containsKey(newtopStateNode)) {
          totalTopStateChangeWithNewDeployment++;
        }
      }
    }

    Map<String, Set<String>> list = convertMapping(map);
    Map<String, Set<String>> newList = convertMapping(newMap);

    Map<String, Integer> addition = new HashMap<>();
    Map<String, Integer> subtraction = new HashMap<>();
    for (String instance : newList.keySet()) {
      Set<String> oldPartitions = list.get(instance);
      Set<String> newPartitions = newList.get(instance);
      totalPartition += newPartitions.size();
      if (oldPartitions == null) {
        addition.put(instance, newPartitions.size());
      } else {
        Set<String> commonPartitions = new HashSet<>(newPartitions);
        commonPartitions.retainAll(oldPartitions);

        newPartitions.removeAll(commonPartitions);

        addition.put(instance, newPartitions.size());

        oldPartitions.removeAll(commonPartitions);
        subtraction.put(instance, oldPartitions.size());
      }
      totalChange += newPartitions.size();
      //System.out.println("Changed partition on node: \t" + instance + "\t: \t" + newPartitions.toString());
    }
    /*
      List<String> instances = new ArrayList<>(newList.keySet());
      Collections.sort(instances);
      System.out.println("Addition partition count: ");
      for (String instance : instances) {
        System.out.println(addition.containsKey(instance) ? addition.get(instance) : 0);
      }

      System.out.println("Subtraction partition count: ");
      for (String instance : instances) {
        System.out.println(subtraction.containsKey(instance) ? subtraction.get(instance) : 0);
      }
    */

    int nodeChanged = 0;
    int necessaryChange = 0;
    int necessarytopStateChange = 0;
    for (String instance : deltaNodes) {
      nodeChanged++;
      if (list.containsKey(instance)) {
        necessaryChange += list.get(instance).size();
        for (Map<String, String> nodeState : map.values()) {
          if (nodeState.containsKey(instance)) {
            if (nodeState.get(instance).equalsIgnoreCase(topState)) {
              necessarytopStateChange++;
            }
          }
        }
      }
      if (newList.containsKey(instance)) {
        necessaryChange += newList.get(instance).size();
        for (Map<String, String> nodeState : newMap.values()) {
          if (nodeState.containsKey(instance)) {
            if (nodeState.get(instance).equalsIgnoreCase(topState)) {
              necessarytopStateChange++;
            }
          }
        }
      }
    }

    if (verbose) {
      System.out.println(
          "\t\t\t" + "Total partition change count: \t" + totalChange + "\t/\t" + totalPartition
              + "\t, rate: \t" + (((float) totalChange) / totalPartition * 100) + "%\t"
              + "Diff nodes have partition \t" + necessaryChange + "\t, unnecessary change rate: \t"
              + (((float) totalChange - necessaryChange) / totalPartition * 100)
              + "%\t, which is \t" + (((float) totalChange - necessaryChange) / totalChange * 100)
              + "%\t of the movement.");
    }

    double expectedAverageMv =
        (double) totalPartition / Math.max(list.size(), newList.size()) * deltaNodes.size();

    return new double[] { nodeChanged, totalChange, (((double) totalChange) / totalPartition * 100),
        (((double) totalChange - necessaryChange) / totalPartition * 100), totalTopStateChange,
        (((double) totalTopStateChange) / totalTopState * 100),
        (((double) totalTopStateChange - necessarytopStateChange) / totalTopState * 100),
        expectedAverageMv, (((double) totalChange - expectedAverageMv) / totalPartition * 100),
        totalTopStateChangeWithNewDeployment,
        (((double) totalTopStateChangeWithNewDeployment) / totalTopState * 100)
    };
  }

  private Map<String, Set<String>> convertMapping(Map<String, Map<String, String>> map) {
    Map<String, Set<String>> list = new HashMap<>();
    for (String partition : map.keySet()) {
      for (String instance : map.get(partition).keySet()) {
        if (!list.containsKey(instance)) {
          list.put(instance, new HashSet<String>());
        }
        list.get(instance).add(partition);
      }
    }
    return list;
  }
}
