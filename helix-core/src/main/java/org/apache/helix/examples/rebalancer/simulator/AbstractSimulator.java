package org.apache.helix.examples.rebalancer.simulator;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.examples.rebalancer.simulator.operations.Operation;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;

/**
 * The abstract simulator class that contains basic logic.
 */
public abstract class AbstractSimulator {
  private final int DEFAULT_WAIT_TIME = 120000;
  private final boolean _verbose;
  private final int _transitionDelay;

  private Map<String, SimulateParticipantProcess> _processMap = new HashMap<>();
  private HelixManager _controller;
  private HelixAdmin _admin;
  private HelixZkClient _zkClient;
  private String _zkServers;
  private String _clusterName;
  private ZkServer _server;
  private Map<String, String> _nodeToZoneMap = new HashMap<>();

  protected HelixAdmin getAdmin() {
    return _admin;
  }

  protected HelixZkClient getZkClient() {
    return _zkClient;
  }

  protected String getClusterName() {
    return _clusterName;
  }

  public Map<String, String> getNodeToZoneMap() {
    return _nodeToZoneMap;
  }

  /**
   * @param transitionDelay state transition delay
   * @param verbose         if true, will output all transition details
   */
  protected AbstractSimulator(int transitionDelay, boolean verbose) {
    _verbose = verbose;
    _transitionDelay = transitionDelay;
  }

  /**
   * @param zkServers             the connection string of the ZkServers that will be used for the simulation
   * @param clusterName           the simulated cluster name
   * @param overwrites            define the overwrites that will be applied before operations
   * @param operations            the simulate operations, cluster status will be output after each operation
   * @param shutdownAfterSimulate if true, the environment will be destroyed immediately after the simulation
   */
  public void simulate(String zkServers, String clusterName, MetadataOverwrites overwrites,
      List<Operation> operations, boolean shutdownAfterSimulate) throws Exception {
    startZookeeper(zkServers);

    _zkServers = zkServers;
    _admin = new ZKHelixAdmin(zkServers);
    _clusterName = clusterName;

    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer()).setConnectInitTimeout(3 * 1000);
    _zkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkServers), clientConfig);

    try {
      if (!prepareMetadata()) {
        echo("Cluster preparation cannot be done. Stop executing operation.");
        return;
      }
      List<String> stateModelDefRefs = _admin.getStateModelDefs(_clusterName);
      setup(stateModelDefRefs, overwrites);
      Set<String> resources = new HashSet<>(_admin.getResourcesInCluster(_clusterName));
      startController();
      HelixClusterVerifier clusterVerifier =
          new StrictMatchExternalViewVerifier.Builder(_clusterName).setResources(resources)
              .setZkAddr(_zkServers).build();
      boolean result = clusterVerifier.verify(DEFAULT_WAIT_TIME);
      if (!result) {
        echo("Cluster does not converge after setup. Stop executing operation.");
        return;
      }
      printState("Cluster initialized.");
      executeScript(operations);
    } finally {
      if (!shutdownAfterSimulate) {
        System.out.println("Press any key to exist...");
        Scanner userInput = new Scanner(System.in);
        userInput.nextLine();
        userInput.close();
      }
      _admin.close();
      if (_controller != null) {
        _controller.disconnect();
      }
      for (SimulateParticipantProcess process : _processMap.values()) {
        process.stop();
      }
      _processMap.clear();
      _zkClient.deleteRecursively("/" + _clusterName);
      _zkClient.close();
      stopZookeeper();
    }
  }

  /**
   * Prepare the cluster metadata before simulating starts.
   *
   * @return true if the preparation is done successfully.
   */
  protected abstract boolean prepareMetadata();

  /**
   * @return true if the cluster is converged.
   */
  protected boolean isClusterConverged() {
    Set<String> resources = new HashSet<>(_admin.getResourcesInCluster(_clusterName));
    HelixClusterVerifier clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(_clusterName).setResources(resources)
            .setZkAddr(_zkServers).build();
    if (!clusterVerifier.verify(DEFAULT_WAIT_TIME * 3)) {
      return false;
    }
    for (String db : getAdmin().getResourcesInCluster(getClusterName())) {
      IdealState is = getAdmin().getResourceIdealState(getClusterName(), db);
      ExternalView ev = getAdmin().getResourceExternalView(getClusterName(), db);
      if (is == null || ev == null) {
        return false;
      } else if (!validateZoneAndTagIsolation(getAdmin(), getClusterName(), is, ev,
          is.getReplicaCount(Math.min(getAdmin().getInstancesInCluster(getClusterName()).size(),
              new HashSet<>(_nodeToZoneMap.values()).size())))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Validate instances for each partition is on different zone and with necessary tagged instances.
   */
  private boolean validateZoneAndTagIsolation(HelixAdmin admin, String clusterName, IdealState is,
      ExternalView ev, int expectedReplicas) {
    String tag = is.getInstanceGroupTag();
    for (String partition : is.getPartitionSet()) {
      Set<String> assignedZones = new HashSet<>();

      Map<String, String> assignmentMap = ev.getRecord().getMapField(partition);
      Set<String> instancesInEV = assignmentMap.keySet();
      for (String instance : instancesInEV) {
        assignedZones.add(_nodeToZoneMap.getOrDefault(instance, instance));
        if (tag != null) {
          InstanceConfig config = admin.getInstanceConfig(clusterName, instance);
          if (!config.containsTag(tag)) {
            return false;
          }
        }
      }
      if (instancesInEV.size() != expectedReplicas) {
        return false;
      }
    }
    return true;
  }

  protected static void echo(Object obj) {
    System.out.println(obj);
  }

  public void startNewProcess(String instanceName, List<String> stateModelDefRefs)
      throws Exception {
    if (_processMap.containsKey(instanceName)) {
      throw new HelixException("Instance already started.");
    }
    SimulateParticipantProcess process =
        new SimulateParticipantProcess(instanceName, stateModelDefRefs, _transitionDelay, _verbose);
    _processMap.put(instanceName, process);
    process.start(_clusterName, _zkServers);
  }

  public void resetProcess(String instanceName) throws Exception {
    SimulateParticipantProcess process = _processMap.get(instanceName);
    if (process != null) {
      process.restart();
    }
  }

  public void stopProcess(String instanceName) {
    SimulateParticipantProcess process = _processMap.remove(instanceName);
    if (process != null) {
      process.stop();
    }
  }

  protected void setup(List<String> stateModelDefRefs, MetadataOverwrites overwrites)
      throws Exception {
    ConfigAccessor configAccessor = new ConfigAccessor(_zkClient);

    List<String> instances = _admin.getInstancesInCluster(_clusterName);
    Map<String, InstanceConfig> instanceConfigMap =
        instances.stream().map(instance -> configAccessor.getInstanceConfig(_clusterName, instance))
            .collect(Collectors.toMap(instanceConfig -> instanceConfig.getInstanceName(),
                instanceConfig -> instanceConfig));

    List<String> resources = _admin.getResourcesInCluster(_clusterName);
    Map<String, IdealState> idealStateMap =
        resources.stream().map(resource -> _admin.getResourceIdealState(_clusterName, resource))
            .collect(Collectors
                .toMap(idealState -> idealState.getResourceName(), idealState -> idealState));

    if (overwrites != null) {
      // Override cluster config
      ClusterConfig newClusterConfig =
          overwrites.updateClusterConfig(configAccessor.getClusterConfig(_clusterName));
      configAccessor.setClusterConfig(_clusterName, newClusterConfig);

      // Override instance config
      Map<String, InstanceConfig> newInstanceConfigMap =
          overwrites.updateInstanceConfig(newClusterConfig, instanceConfigMap);
      // remove all the non-exist instances
      Map<String, InstanceConfig> removingInstances = new HashMap<>(instanceConfigMap);
      removingInstances.keySet().removeAll(newInstanceConfigMap.keySet());
      for (InstanceConfig config : removingInstances.values()) {
        _admin.dropInstance(_clusterName, config);
      }
      // update the new instance configs
      for (InstanceConfig config : newInstanceConfigMap.values()) {
        _admin.setInstanceConfig(_clusterName, config.getInstanceName(), config);
      }
      instanceConfigMap = newInstanceConfigMap;

      // Override resource idealState
      Map<String, IdealState> newIdealStateMap = overwrites.updateIdealStates(idealStateMap);
      // remove all the non-exist resources
      Map<String, IdealState> removingIdealStates = new HashMap<>(idealStateMap);
      removingIdealStates.keySet().removeAll(newIdealStateMap.keySet());
      for (String resource : removingIdealStates.keySet()) {
        _admin.dropResource(_clusterName, resource);
      }
      // update the new resource idealStates
      for (IdealState is : newIdealStateMap.values()) {
        _admin.setResourceIdealState(_clusterName, is.getResourceName(), is);
      }
      idealStateMap = newIdealStateMap;

      // Override resource config
      resources = _admin.getResourcesInCluster(_clusterName);
      Map<String, ResourceConfig> resourceConfigMap = resources.stream().map(resource -> {
        ResourceConfig config = configAccessor.getResourceConfig(_clusterName, resource);
        if (config == null) {
          config = new ResourceConfig(resource);
        }
        return config;
      }).collect(Collectors.toMap(resourceConfig -> resourceConfig.getResourceName(),
          resourceConfig -> resourceConfig));
      Map<String, ResourceConfig> newResourceConfigMap =
          overwrites.updateResourceConfigs(resourceConfigMap, newIdealStateMap);
      // remove all the non-exist resources
      Map<String, ResourceConfig> removingResourceConfigs = new HashMap<>(resourceConfigMap);
      removingResourceConfigs.keySet().removeAll(newResourceConfigMap.keySet());
      for (String resource : removingResourceConfigs.keySet()) {
        HelixConfigScope resourceScope =
            new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
                .forCluster(_clusterName).forResource(resource).build();
        configAccessor.remove(resourceScope, resource);
      }
      // update the new resource configs
      for (ResourceConfig config : newResourceConfigMap.values()) {
        configAccessor.setResourceConfig(_clusterName, config.getResourceName(), config);
      }
    }

    ClusterConfig clusterConfig = configAccessor.getClusterConfig(_clusterName);
    clusterConfig.setPersistBestPossibleAssignment(true);
    configAccessor.setClusterConfig(_clusterName, clusterConfig);

    for (IdealState is : idealStateMap.values()) {
      _admin.rebalance(_clusterName, is.getResourceName(),
          is.getReplicaCount(instanceConfigMap.size()));
    }

    for (InstanceConfig instanceConfig : instanceConfigMap.values()) {
      startNewProcess(instanceConfig.getInstanceName(), stateModelDefRefs);
      _nodeToZoneMap.put(instanceConfig.getInstanceName(), instanceConfig.getZoneId());
    }
  }

  private void executeScript(List<Operation> operations) throws IOException {
    for (Operation operation : operations) {
      echo("Start operation: " + operation.getDescription());
      // Reset the count before operation.
      for (SimulateParticipantProcess process : _processMap.values()) {
        process.getAndResetTransitions();
      }
      if (operation.execute(_admin, _zkClient, _clusterName)) {
        if (!_admin.isInMaintenanceMode(_clusterName)) {
          ClusterStateVerifier.verifyByPolling(() -> {
            for (SimulateParticipantProcess process : _processMap.values()) {
              if (process.getTransitionCount() > 0) {
                return true;
              }
            }
            return false;
          }, DEFAULT_WAIT_TIME);
        }

        if (!isClusterConverged()) {
          echo("Cluster does not converge after operation " + operation.getDescription()
              + ". Stop the next operation.");
          break;
        }
        printState("Done operation: " + operation.getDescription());
      } else {
        echo("Operation " + operation.getDescription() + " failed. Stop the next operation.");
        break;
      }
    }
  }

  private void printState(String desc) throws IOException {
    echo(desc);

    ConfigAccessor configAccessor = new ConfigAccessor(_zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(_clusterName);
    List<String> capacityKeys = clusterConfig.getInstanceCapacityKeys();

    Set<String> resources = new HashSet<>(_admin.getResourcesInCluster(_clusterName));

    Map<String, Integer> partitionCount = new HashMap<>();
    Map<String, Map<String, Integer>> perStatePartitionCount = new HashMap<>();
    Map<String, Map<String, Integer>> perCapacityKeyResourceUsage = new HashMap<>();
    for (String key : capacityKeys) {
      perCapacityKeyResourceUsage.put(key, new HashMap<>());
    }

    for (String resource : resources) {
      ResourceConfig resourceConfig = configAccessor.getResourceConfig(_clusterName, resource);
      if (resourceConfig == null) {
        resourceConfig = new ResourceConfig(resource);
      }
      Map<String, Map<String, Integer>> capacityMap = resourceConfig.getPartitionCapacityMap();

      ExternalView ev = _admin.getResourceExternalView(_clusterName, resource);
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        for (String instance : stateMap.keySet()) {
          // update partition count
          partitionCount.put(instance, partitionCount.getOrDefault(instance, 0) + 1);
          String state = stateMap.get(instance);
          Map<String, Integer> statePartitionCount =
              perStatePartitionCount.computeIfAbsent(state, key -> new HashMap<>());
          statePartitionCount.put(instance, statePartitionCount.getOrDefault(instance, 0) + 1);
          // update capacity usage
          Map<String, Integer> partitionCapacity = capacityMap.get(partition);
          if (partitionCapacity == null) {
            partitionCapacity = capacityMap.get(ResourceConfig.DEFAULT_PARTITION_KEY);
          }
          if (partitionCapacity == null) {
            partitionCapacity = new HashMap<>();
          }
          for (String capacityKey : partitionCapacity.keySet()) {
            Map<String, Integer> capacityUsageMap =
                perCapacityKeyResourceUsage.computeIfAbsent(capacityKey, key -> new HashMap<>());
            capacityUsageMap.put(instance,
                capacityUsageMap.getOrDefault(instance, 0) + partitionCapacity.get(capacityKey));
          }
        }
      }
    }

    SummaryStatistics stats = new SummaryStatistics();
    for (int pcount : partitionCount.values()) {
      stats.addValue(pcount);
    }
    echo(String
        .format("Partition distribution: Max %f, Min %f, STDEV %f", stats.getMax(), stats.getMin(),
            stats.getStandardDeviation()));

    for (String state : perStatePartitionCount.keySet()) {
      stats = new SummaryStatistics();
      for (int mcount : perStatePartitionCount.get(state).values()) {
        stats.addValue(mcount);
      }
      echo(String
          .format(state + " distribution: Max %f, Min %f, STDEV %f", stats.getMax(), stats.getMin(),
              stats.getStandardDeviation()));
    }

    for (String capacityKey : perCapacityKeyResourceUsage.keySet()) {
      stats = new SummaryStatistics();
      for (int mcount : perCapacityKeyResourceUsage.get(capacityKey).values()) {
        stats.addValue(mcount);
      }
      echo(String
          .format("Capacity Usage " + capacityKey + " distribution: Max %f, Min %f, STDEV %f",
              stats.getMax(), stats.getMin(), stats.getStandardDeviation()));
    }

    Map<String, Integer> totalTransitionCount = new HashMap<>();
    for (SimulateParticipantProcess process : _processMap.values()) {
      Map<String, Integer> transitionCount = process.getAndResetTransitions();
      if (_verbose) {
        echo(String
            .format("Transitions Done on %s for this operation: %s", process.getInstanceName(),
                transitionCount.toString()));
      }
      transitionCount.forEach(
          (k, v) -> totalTransitionCount.put(k, totalTransitionCount.getOrDefault(k, 0) + v));
    }
    echo("Total Transitions Done for this operation: " + totalTransitionCount.toString());
  }

  private void startController() {
    // start controller
    echo("Starting Helix Controller.");
    _controller = HelixControllerMain
        .startHelixController(_zkServers, _clusterName, "localhost_9100",
            HelixControllerMain.STANDALONE);
  }

  private void startZookeeper(String zkServers) throws IOException {
    echo("STARTING Zookeeper at " + zkServers);
    String zkFolderName = "helix-rebalanceSimulator-ZK";
    IDefaultNameSpace defaultNameSpace = zkClient -> {
    };
    File file = new File("/tmp/" + zkFolderName);
    if (file.exists()) {
      if (file.isDirectory()) {
        FileUtils.deleteDirectory(file);
      } else {
        file.deleteOnExit();
      }
    }
    file.mkdirs();
    // start zookeeper
    _server = new ZkServer("/tmp/" + zkFolderName + "/dataDir", "/tmp/" + zkFolderName + "/logDir",
        defaultNameSpace, 2199);
    _server.start();
  }

  private void stopZookeeper() {
    if (_server != null) {
      _server.shutdown();
    }
  }

  /**
   * The mock participant process to simulate a live Helix participant.
   */
  private class SimulateParticipantProcess {
    private final String _instanceName;
    private final int _transitionDelay;
    private final boolean _verbose;
    private final List<String> _stateModelDefRefs;

    private HelixManager _manager;
    private WildcardStateModelFactory _factory;

    /**
     * @param instanceName      instance name.
     * @param stateModelDefRefs all the state model defs that are supported by the participant.
     * @param transitionDelay   the simulated latency of state transition.
     * @param verbose           if true, print more detailed state transition logs.
     */
    SimulateParticipantProcess(String instanceName, List<String> stateModelDefRefs,
        int transitionDelay, boolean verbose) {
      _instanceName = instanceName;
      _transitionDelay = transitionDelay;
      _verbose = verbose;
      _stateModelDefRefs = new ArrayList<>(stateModelDefRefs);
    }

    void start(String clusterName, String zkServers) throws Exception {
      _manager = HelixManagerFactory
          .getZKHelixManager(clusterName, _instanceName, InstanceType.PARTICIPANT, zkServers);
      _factory = new WildcardStateModelFactory(_instanceName, _transitionDelay, _verbose);
      StateMachineEngine stateMach = _manager.getStateMachineEngine();
      for (String ref : _stateModelDefRefs) {
        stateMach.registerStateModelFactory(ref, _factory);
      }
      _manager.connect();
    }

    void stop() {
      _manager.disconnect();
    }

    void restart() throws Exception {
      String clusterName = _manager.getClusterName();
      String zkServers = _manager.getMetadataStoreConnectionString();
      stop();
      start(clusterName, zkServers);
    }

    int getTransitionCount() {
      return _factory.getTransitionCount();
    }

    Map<String, Integer> getAndResetTransitions() {
      return _factory.getAndResetTransitionRecords();
    }

    String getInstanceName() {
      return _instanceName;
    }
  }
}
