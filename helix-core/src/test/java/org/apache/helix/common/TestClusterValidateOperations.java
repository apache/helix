package org.apache.helix.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;


public final class TestClusterValidateOperations {

  /**
   * Validate there should be always minimal active replica and top state replica for each
   * partition.
   * Also make sure there is always some partitions with only active replica count.
   */
  public static void validateMinActiveAndTopStateReplica(IdealState is, ExternalView ev,
      int minActiveReplica, int numNodes) {
    StateModelDefinition stateModelDef =
        BuiltInStateModelDefinitions.valueOf(is.getStateModelDefRef()).getStateModelDefinition();
    String topState = stateModelDef.getStatesPriorityList().get(0);
    int replica = Integer.valueOf(is.getReplicas());

    Map<String, Integer> stateCount = stateModelDef.getStateCountMap(numNodes, replica);
    Set<String> activeStates = stateCount.keySet();

    for (String partition : is.getPartitionSet()) {
      Map<String, String> assignmentMap = ev.getRecord().getMapField(partition);
      Assert.assertNotNull(assignmentMap,
          is.getResourceName() + "'s best possible assignment is null for partition " + partition);
      Assert.assertTrue(!assignmentMap.isEmpty(),
          is.getResourceName() + "'s partition " + partition + " has no best possible map in IS.");

      boolean hasTopState = false;
      int activeReplica = 0;
      for (String state : assignmentMap.values()) {
        if (topState.equalsIgnoreCase(state)) {
          hasTopState = true;
        }
        if (activeStates.contains(state)) {
          activeReplica++;
        }
      }

      if (activeReplica < minActiveReplica) {
        int a = 0;
      }

      Assert.assertTrue(hasTopState, String.format("%s missing %s replica", partition, topState));
      Assert.assertTrue(activeReplica >= minActiveReplica,
          String.format("%s has less active replica %d then required %d", partition, activeReplica,
              minActiveReplica));
    }
  }

  public static void verifyInstance(HelixZkClient zkClient, String clusterName, String instance,
      boolean wantExists) {
    // String instanceConfigsPath = HelixUtil.getConfigPath(clusterName);
    String instanceConfigsPath = PropertyPathBuilder.instanceConfig(clusterName);
    String instanceConfigPath = instanceConfigsPath + "/" + instance;
    String instancePath = PropertyPathBuilder.instance(clusterName, instance);
    Assert.assertEquals(wantExists, zkClient.exists(instanceConfigPath));
    Assert.assertEquals(wantExists, zkClient.exists(instancePath));
  }

  public static void verifyResource(HelixZkClient zkClient, String clusterName, String resource,
      boolean wantExists) {
    String resourcePath = PropertyPathBuilder.idealState(clusterName, resource);
    Assert.assertEquals(wantExists, zkClient.exists(resourcePath));
  }

  public static void verifyEnabled(HelixZkClient zkClient, String clusterName, String instance,
      boolean wantEnabled) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instance));
    Assert.assertEquals(wantEnabled, config.getInstanceEnabled());
  }

  public static void verifyReplication(HelixZkClient zkClient, String clusterName, String resource,
      int repl) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resource));
    for (String partitionName : idealState.getPartitionSet()) {
      if (idealState.getRebalanceMode() == IdealState.RebalanceMode.SEMI_AUTO) {
        Assert.assertEquals(repl, idealState.getPreferenceList(partitionName).size());
      } else if (idealState.getRebalanceMode() == IdealState.RebalanceMode.CUSTOMIZED) {
        Assert.assertEquals(repl, idealState.getInstanceStateMap(partitionName).size());
      }
    }
  }

  public static boolean verifyBalanceExternalView(ZNRecord externalView, int partitionCount,
      String masterState, int replica, int instances) {
    return verifyBalanceExternalView(externalView, partitionCount, masterState, replica, instances, null);
  }

  public static boolean verifyBalanceExternalView(ZNRecord externalView, int partitionCount,
      String masterState, int replica, int instances, Integer maxPerInstance) {
    Map<String, Integer> masterPartitionsCountMap = new HashMap<>();
    for (String partitionName : externalView.getMapFields().keySet()) {
      Map<String, String> assignmentMap = externalView.getMapField(partitionName);
      for (String instance : assignmentMap.keySet()) {
        if (assignmentMap.get(instance).equals(masterState)) {
          if (!masterPartitionsCountMap.containsKey(instance)) {
            masterPartitionsCountMap.put(instance, 0);
          }
          masterPartitionsCountMap.put(instance, masterPartitionsCountMap.get(instance) + 1);
        }
      }
    }

    int perInstancePartition = partitionCount / instances;

    int totalCount = 0;
    for (String instanceName : masterPartitionsCountMap.keySet()) {
      Integer instancePartitionCount = masterPartitionsCountMap.get(instanceName);
      totalCount += instancePartitionCount;
      if (!(instancePartitionCount == perInstancePartition
          || instancePartitionCount == perInstancePartition + 1
          || (maxPerInstance == null || instancePartitionCount == maxPerInstance))) {
        return false;
      }
      if (maxPerInstance != null && instancePartitionCount == maxPerInstance) {
        continue;
      }
      if (instancePartitionCount == perInstancePartition + 1) {
        if (partitionCount % instances == 0) {
          return false;
        }
      }
    }
    if (maxPerInstance != null && totalCount == maxPerInstance * instances) {
      return true;
    }
    return partitionCount == totalCount;
  }

}
