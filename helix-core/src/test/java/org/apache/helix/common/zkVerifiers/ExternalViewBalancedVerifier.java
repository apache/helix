package org.apache.helix.common.zkVerifiers;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExternalViewBalancedVerifier implements ClusterStateVerifier.ZkVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalViewBalancedVerifier.class);

  HelixZkClient _client;
  String _clusterName;
  String _resourceName;

  public ExternalViewBalancedVerifier(HelixZkClient client, String clusterName,
      String resourceName) {
    _client = client;
    _clusterName = clusterName;
    _resourceName = resourceName;
  }

  @Override
  public boolean verify() {
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<>(_client));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    int numberOfPartitions;
    try {
      numberOfPartitions = accessor.getProperty(keyBuilder.idealStates(_resourceName)).getRecord()
          .getListFields().size();
    } catch (Exception e) {
      return false;
    }
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
    cache.refresh(accessor);

    IdealState idealState = cache.getIdealState(_resourceName);
    if (idealState == null) {
      return false;
    }
    String masterValue =
        cache.getStateModelDef(idealState.getStateModelDefRef()).getStatesPriorityList().get(0);
    int replicas = Integer.parseInt(cache.getIdealState(_resourceName).getReplicas());
    String instanceGroupTag = cache.getIdealState(_resourceName).getInstanceGroupTag();
    int instances = 0;
    for (String liveInstanceName : cache.getLiveInstances().keySet()) {
      if (cache.getInstanceConfigMap().get(liveInstanceName).containsTag(instanceGroupTag)) {
        instances++;
      }
    }
    if (instances == 0) {
      instances = cache.getLiveInstances().size();
    }
    ExternalView ev = accessor.getProperty(keyBuilder.externalView(_resourceName));
    if (ev == null) {
      return false;
    }
    return verify(ev.getRecord(), numberOfPartitions, masterValue, replicas, instances);
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
      String masterState, int replica, int instances) {
    if (externalView == null) {
      return false;
    }
    Map<String, Integer> masterPartitionsCountMap = new HashMap<>();
    for (String partitionName : externalView.getMapFields().keySet()) {
      Map<String, String> assignmentMap = externalView.getMapField(partitionName);
      // Assert.assertTrue(assignmentMap.size() >= replica);
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
          || instancePartitionCount == perInstancePartition + 1)) {
        return false;
      }
      if (instancePartitionCount == perInstancePartition + 1) {
        if (partitionCount % instances == 0) {
          return false;
        }
      }
    }
    return partitionCount == totalCount;
  }
}
