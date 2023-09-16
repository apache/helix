package org.apache.helix.common.zkVerifiers;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExternalViewBalancedVerifierWithMaxPartitions implements ClusterStateVerifier.ZkVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalViewBalancedVerifierWithMaxPartitions.class);

  HelixZkClient _client;
  String _clusterName;
  String _resourceName;

  public ExternalViewBalancedVerifierWithMaxPartitions(HelixZkClient client, String clusterName, String resourceName) {
    _client = client;
    _clusterName = clusterName;
    _resourceName = resourceName;
  }

  @Override
  public boolean verify() {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<>(_client));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    int numberOfPartitions = accessor.getProperty(keyBuilder.idealStates(_resourceName))
        .getRecord().getListFields().size();
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
    cache.refresh(accessor);
    String masterValue =
        cache.getStateModelDef(cache.getIdealState(_resourceName).getStateModelDefRef())
            .getStatesPriorityList().get(0);
    int replicas = Integer.parseInt(cache.getIdealState(_resourceName).getReplicas());
    try {
      return verify(
          accessor.getProperty(keyBuilder.externalView(_resourceName)).getRecord(),
          numberOfPartitions, masterValue, replicas, cache.getLiveInstances().size(),
          cache.getIdealState(_resourceName).getMaxPartitionsPerInstance());
    } catch (Exception e) {
      LOG.debug("Verify failed", e);
      return false;
    }
  }

  @Override
  public ZkClient getZkClient() {
    return (ZkClient) _client;
  }

  @Override
  public String getClusterName() {
    return _clusterName;
  }

  private static boolean verify(ZNRecord externalView, int partitionCount,
      String masterState, int replica, int instances, int maxPerInstance) {
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
      int instancePartitionCount = masterPartitionsCountMap.get(instanceName);
      totalCount += instancePartitionCount;
      if (!(instancePartitionCount == perInstancePartition
          || instancePartitionCount == perInstancePartition + 1
          || instancePartitionCount == maxPerInstance)) {
        return false;
      }
      if (instancePartitionCount == maxPerInstance) {
        continue;
      }
      if (instancePartitionCount == perInstancePartition + 1) {
        if (partitionCount % instances == 0) {
          return false;
        }
      }
    }
    if (totalCount == maxPerInstance * instances) {
      return true;
    }
    return partitionCount == totalCount;
  }
}