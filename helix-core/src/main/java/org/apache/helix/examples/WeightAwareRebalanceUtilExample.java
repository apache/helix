package org.apache.helix.examples;

import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.HelixAdmin;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceHardConstraint;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceSoftConstraint;
import org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider;
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.rebalancer.constraint.PartitionWeightAwareEvennessConstraint;
import org.apache.helix.controller.rebalancer.constraint.TotalCapacityConstraint;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.ZkBasedCapacityProvider;
import org.apache.helix.controller.rebalancer.constraint.dataprovider.ZkBasedPartitionWeightProvider;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.util.WeightAwareRebalanceUtil;

import java.util.*;

public class WeightAwareRebalanceUtilExample {
  private static String ZK_ADDRESS = "localhost:2199";
  private static String CLUSTER_NAME = "RebalanceUtilExampleCluster";
  private static HelixAdmin admin;

  final static String resourceNamePrefix = "resource";
  final static int nParticipants = 9;
  final static int nResources = 10;
  final static int nPartitions = 10;
  final static int nReplicas = 3;
  final static int defaultCapacity = 500; // total = 500*7 = 3500
  final static int resourceWeight = 10; // total = 10*10*3*10 = 3000

  final static List<String> resourceNames = new ArrayList<>();
  final static List<String> instanceNames = new ArrayList<>();
  final static List<String> partitions = new ArrayList<>(nPartitions);
  final static List<ResourceConfig> resourceConfigs = new ArrayList<>();

  final static ClusterConfig clusterConfig = new ClusterConfig(CLUSTER_NAME);
  final static List<InstanceConfig> instanceConfigs = new ArrayList<>();

  private static void printAssignmentInfo(ResourcesStateMap assignment) {
    System.out.println("The result assignment is: ");
    Map<String, Integer> instanceLoad = new HashMap<>();
    for (String resource : assignment.getResourceStatesMap().keySet()) {
      System.out.println(resource + ": " + assignment.getPartitionStateMap(resource).toString());
      for (Map<String, String> stateMap : assignment.getPartitionStateMap(resource).getStateMap().values()) {
        for (String instance : stateMap.keySet()) {
          if (!instanceLoad.containsKey(instance)) {
            instanceLoad.put(instance, 0);
          }
          instanceLoad.put(instance, instanceLoad.get(instance) + resourceWeight);
        }
      }
    }
    System.out.println("Instance load: " + instanceLoad + "\n");
  }

  private static void rebalanceUtilUsage() {
    System.out.println(String.format("Start rebalancing using WeightAwareRebalanceUtil for %d resources.", nResources));

    /**
     * Init providers with resource weight and participant capacity.
     *
     * Users should have their own logic to determine this information for their applications.
     */
    PartitionWeightProvider weightProvider = new PartitionWeightProvider() {
      @Override
      public int getPartitionWeight(String resource, String partition) {
        return resourceWeight;
      }
    };
    CapacityProvider capacityProvider = new CapacityProvider() {
      @Override
      public int getParticipantCapacity(String participant) {
        return defaultCapacity;
      }

      @Override
      public int getParticipantUsage(String participant) {
        return 0;
      }
    };

    /**
     * Init constraints with providers
     * In this example, we used 2 constraints.
     *
     * capacityConstraint will ensure the participant capacity limitation is not exceed.
     * evenConstraint will ensure the distribution is even through all eligible participant.
     */
    TotalCapacityConstraint capacityConstraint =
        new TotalCapacityConstraint(weightProvider, capacityProvider);
    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    /**
     * Call the util to calculate partition assignment
     *
     * Note that clusterConfig and instanceConfigs are predefined with minimized configuration set.
     * Users can also read this information from cluster's ZK nodes using ConfigAccessor.
     * @see org.apache.helix.ConfigAccessor#getInstanceConfig(String, String)
     * @see org.apache.helix.ConfigAccessor#getClusterConfig(String)
     */
    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);

    /**
     * For completely new assignment, call buildIncrementalRebalanceAssignment with no existingAssignment specified.
     *
     * Note that user needs to build resrouceConfigs before using the tool.
     * If the config exists in ZK, the same object can be used directly.
     * @see org.apache.helix.ConfigAccessor#getResourceConfig(String, String)
     */
    ResourcesStateMap assignment = util.buildIncrementalRebalanceAssignment(resourceConfigs, null,
        Collections.<AbstractRebalanceHardConstraint>singletonList(capacityConstraint),
        Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));
    System.out.println(String.format("Finished rebalancing using WeightAwareRebalanceUtil for %d resources.", nResources));
    printAssignmentInfo(assignment);
  }

  private static void rebalanceUtilUsageWithZkBasedDataProvider() {
    System.out.println(String.format("Start rebalancing using WeightAwareRebalanceUtil and ZK based Capacity/Weight data providers for %d resources.", nResources));

    // Init a zkserver & cluster nodes for this example
    ZkServer zkServer = ExampleHelper.startZkServer(ZK_ADDRESS);
    admin = new ZKHelixAdmin(ZK_ADDRESS);
    admin.addCluster(CLUSTER_NAME, true);

    /**
     * In order to avoid re-construct capacity / usage information every time, user can choose to use ZK based providers.
     * In this example, we assume the evaluating metrics are QPS and memory.
     * In this case, 2 sets of constraints are needed.
     *
     * Init and persistent ZkBasedDataProvider.
     * 1. Create ZK based providers and init with capacity / weight information.
     * 2. Persist providers in to HelixPropertyStore.
     * 3. Read from ZK when necessary.
     */
    // For QPS
    ZkBasedPartitionWeightProvider qpsWeightProvider =
        new ZkBasedPartitionWeightProvider(ZK_ADDRESS, CLUSTER_NAME, "QPS");
    // Note that user can specify more detailed weight info for each partition.
    qpsWeightProvider.updateWeights(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceWeight);
    ZkBasedCapacityProvider qpsCapacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDRESS, CLUSTER_NAME, "QPS");
    qpsCapacityProvider.updateCapacity(Collections.EMPTY_MAP, Collections.EMPTY_MAP, defaultCapacity);
    // For Memory
    ZkBasedPartitionWeightProvider memoryWeightProvider =
        new ZkBasedPartitionWeightProvider(ZK_ADDRESS, CLUSTER_NAME, "MEM");
    // Note that user can specify more detailed capacity and usage info for each participant.
    memoryWeightProvider.updateWeights(Collections.EMPTY_MAP, Collections.EMPTY_MAP, resourceWeight);
    ZkBasedCapacityProvider memoryCapacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDRESS, CLUSTER_NAME, "MEM");
    memoryCapacityProvider.updateCapacity(Collections.EMPTY_MAP, Collections.EMPTY_MAP, defaultCapacity);

    // Persist providers
    qpsCapacityProvider.persistCapacity();
    qpsWeightProvider.persistWeights();
    memoryCapacityProvider.persistCapacity();
    memoryWeightProvider.persistWeights();

    /**
     * Init constraints with ZkBasedDataProvider
     * 1. Read providers from ZK by constructing the object with same ZK address, cluster name, and dimension name
     * 2. Specify constraints with the provider. Only use soft constraint here for simplifying.
     */
    qpsWeightProvider =
        new ZkBasedPartitionWeightProvider(ZK_ADDRESS, CLUSTER_NAME, "QPS");
    qpsCapacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDRESS, CLUSTER_NAME, "QPS");
    memoryWeightProvider =
        new ZkBasedPartitionWeightProvider(ZK_ADDRESS, CLUSTER_NAME, "MEM");
    memoryCapacityProvider =
        new ZkBasedCapacityProvider(ZK_ADDRESS, CLUSTER_NAME, "MEM");

    // !WARNING! Don't put providers that are not providing same type of data
    PartitionWeightAwareEvennessConstraint qpsConstraint =
        new PartitionWeightAwareEvennessConstraint(qpsWeightProvider, qpsCapacityProvider);
    PartitionWeightAwareEvennessConstraint memoryConstraint =
        new PartitionWeightAwareEvennessConstraint(memoryWeightProvider, memoryCapacityProvider);

    List<AbstractRebalanceSoftConstraint> softConstraints = new ArrayList<>();
    softConstraints.add(qpsConstraint);
    softConstraints.add(memoryConstraint);

    /**
     * Call util to calculate partition assignment.
     * Here, use the same simple config set for example. User can always customize the configs.
     */
    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);
    ResourcesStateMap assignment =
        util.buildIncrementalRebalanceAssignment(resourceConfigs, null, Collections.EMPTY_LIST,
            softConstraints);

    ExampleHelper.stopZkServer(zkServer);

    System.out.println(String.format("Finished rebalancing using WeightAwareRebalanceUtil and ZK based Capacity/Weight data providers for %d resources.", nResources));
    printAssignmentInfo(assignment);
  }

  private static void rebalanceWithFaultZone() {
    System.out.println(String.format("Start rebalancing using WeightAwareRebalanceUtil for %d resources in a topology aware cluster.", nResources));

    /**
     * Setup cluster config and instance configs for topology information
     * 1. enable topology aware rebalance
     * 2. setup topology layout and faultzone type
     * 3. for each instance configure domain info
     *
     * If these information is already in ZK, user can read from ZK directly.
     * @see org.apache.helix.ConfigAccessor#getClusterConfig(String)
     * @see org.apache.helix.ConfigAccessor#getInstanceConfig(String, String)
     */
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/Rack/Host");
    clusterConfig.setFaultZoneType("Rack");

    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    for (int i = 0; i < instanceNames.size(); i++) {
      String instance = instanceNames.get(i);
      InstanceConfig config = new InstanceConfig(instance);
      String domainStr = String.format("Rack=%s,Host=%s", i % (nParticipants / 3), instance);
      config.setDomain(domainStr);
      instanceConfigs.add(config);
      System.out.println(String.format("Set instance %s domain to be %s.", instance, domainStr));
    }

    /**
     * Init constraints in the same way as the previous examples.
     */
    PartitionWeightProvider weightProvider = new PartitionWeightProvider() {
      @Override
      public int getPartitionWeight(String resource, String partition) {
        return resourceWeight;
      }
    };
    CapacityProvider capacityProvider = new CapacityProvider() {
      @Override
      public int getParticipantCapacity(String participant) {
        return defaultCapacity;
      }

      @Override
      public int getParticipantUsage(String participant) {
        return 0;
      }
    };
    TotalCapacityConstraint capacityConstraint =
        new TotalCapacityConstraint(weightProvider, capacityProvider);
    PartitionWeightAwareEvennessConstraint evenConstraint =
        new PartitionWeightAwareEvennessConstraint(weightProvider, capacityProvider);

    /**
     * Call the util to calculate partition assignment
     */
    WeightAwareRebalanceUtil util = new WeightAwareRebalanceUtil(clusterConfig, instanceConfigs);

    ResourcesStateMap assignment = util.buildIncrementalRebalanceAssignment(resourceConfigs, null,
        Collections.<AbstractRebalanceHardConstraint>singletonList(capacityConstraint),
        Collections.<AbstractRebalanceSoftConstraint>singletonList(evenConstraint));

    System.out.println(String.format("Finished rebalancing using WeightAwareRebalanceUtil for %d resources in a topology aware cluster with %d fault zones.", nResources, nParticipants / 3));
    printAssignmentInfo(assignment);
  }

  private static void setup() {
    for (int i = 0; i < nParticipants; i++) {
      instanceNames.add("node" + i);
    }
    for (int i = 0; i < nPartitions; i++) {
      partitions.add(Integer.toString(i));
    }

    for (int i = 0; i < nResources; i++) {
      String resourceName = resourceNamePrefix + i;
      resourceNames.add(resourceName);
      ResourceConfig.Builder resourceBuilder = new ResourceConfig.Builder(resourceName);
      resourceBuilder.setStateModelDefRef("MasterSlave");
      resourceBuilder.setNumReplica(nReplicas);
      for (String partition : partitions) {
        resourceBuilder.setPreferenceList(partition, Collections.EMPTY_LIST);
      }
      resourceConfigs.add(resourceBuilder.build());
    }

    for (String instance : instanceNames) {
      InstanceConfig config = new InstanceConfig(instance);
      instanceConfigs.add(config);
    }
  }

  public static void main(String[] args) throws Exception {
    setup();
    rebalanceUtilUsage();
    rebalanceUtilUsageWithZkBasedDataProvider();
    rebalanceWithFaultZone();
  }
}
