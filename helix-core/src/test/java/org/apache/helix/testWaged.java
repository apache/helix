package org.apache.helix;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.helix.model.ResourceConfig.DEFAULT_PARTITION_KEY;

public class testWaged {
  @Test
  public void dummy() throws Exception {
    String zkaddr = "localhost:12913";
    String cluster = "ESPRESSO_MT-LD-5";

    BaseDataAccessor<ZNRecord> baseDataAccessor =
        new ZkBaseDataAccessor<>(zkaddr, new ZNRecordSerializer());
    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(cluster, baseDataAccessor);
    ClusterConfig clusterConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setGlobalRebalanceAsyncMode(false);
    List<InstanceConfig> instanceConfigs =
        dataAccessor.getChildValues(dataAccessor.keyBuilder().instanceConfigs(), true);
    List<String> liveInstances =
        dataAccessor.getChildNames(dataAccessor.keyBuilder().liveInstances());
    List<IdealState> idealStates = new ArrayList<>();
    for (HelixProperty idealStateProperty : dataAccessor
        .getChildValues(dataAccessor.keyBuilder().idealStates(), true)) {
      IdealState idealState = (IdealState) idealStateProperty;
      if (idealState.getRebalanceMode().equals(IdealState.RebalanceMode.FULL_AUTO) && idealState
          .getStateModelDefRef().equals("MasterSlave")) {
        idealState.setRebalancerClassName(WagedRebalancer.class.getName());
        idealStates.add(idealState);
      }
    }
    List<ResourceConfig> resourceConfigs =
        dataAccessor.getChildValues(dataAccessor.keyBuilder().resourceConfigs(), true);

    int movement = 0;
    int statechange = 0;
    int totalReplica = 0;

    // Verify that utilResult contains the assignment for the resources added
    Map<String, ResourceAssignment> utilResult = HelixUtil
        .getTargetAssignmentForWagedFullAuto(zkaddr, clusterConfig, instanceConfigs, liveInstances,
            idealStates, resourceConfigs);

    for (IdealState is : idealStates) {
      String resource = is.getResourceName();
      ExternalView ev = dataAccessor.getProperty(dataAccessor.keyBuilder().externalView(resource));
      ResourceAssignment assignment = utilResult.get(resource);
      Assert.assertEquals(ev.getPartitionSet().size(), assignment.getMappedPartitions().size());

      for (String partition : ev.getPartitionSet()) {
        Map<String, String> evStateMap = ev.getStateMap(partition);
        Map<String, String> newStateMap = assignment.getReplicaMap(new Partition(partition));
        for (String node : newStateMap.keySet()) {
          totalReplica++;
          if (evStateMap.containsKey(node)) {
            if (!evStateMap.get(node).equals(newStateMap.get(node))) {
              statechange++;
            }
          } else {
            movement++;
          }
        }
      }
      System.out.println("Total replica count: " + (ev.getPartitionSet().size() * 3));
    }

    System.out.println(
        "After migration, movement: " + movement + ", pure state transition: " + statechange
            + ", total replica: " + totalReplica);
  }
}
