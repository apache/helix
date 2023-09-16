package org.apache.helix.common.zkVerifiers;

import java.util.List;
import java.util.Map;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Ensures that external view and current state are empty
 */
public class EmptyZkVerifier implements ClusterStateVerifier.ZkVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(EmptyZkVerifier.class);

  private final String _clusterName;
  private final String _resourceName;
  private final HelixZkClient _zkClient;

  /**
   * Instantiate the verifier
   * @param clusterName the cluster to verify
   * @param resourceName the resource to verify
   */
  public EmptyZkVerifier(String clusterName, String resourceName) {
    _clusterName = clusterName;
    _resourceName = resourceName;

    _zkClient = DedicatedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(ZkTestBase.ZK_ADDR));
    _zkClient.setZkSerializer(new ZNRecordSerializer());
  }

  @Override
  public boolean verify() {
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(_clusterName, baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    ExternalView externalView = accessor.getProperty(keyBuilder.externalView(_resourceName));

    // verify external view empty
    if (externalView != null) {
      for (String partition : externalView.getPartitionSet()) {
        Map<String, String> stateMap = externalView.getStateMap(partition);
        if (stateMap != null && !stateMap.isEmpty()) {
          LOG.error("External view not empty for " + partition);
          return false;
        }
      }
    }

    // verify current state empty
    List<String> liveParticipants = accessor.getChildNames(keyBuilder.liveInstances());
    for (String participant : liveParticipants) {
      List<String> sessionIds = accessor.getChildNames(keyBuilder.sessions(participant));
      for (String sessionId : sessionIds) {
        CurrentState currentState =
            accessor.getProperty(keyBuilder.currentState(participant, sessionId, _resourceName));
        Map<String, String> partitionStateMap = currentState.getPartitionStateMap();
        if (partitionStateMap != null && !partitionStateMap.isEmpty()) {
          LOG.error("Current state not empty for " + participant);
          return false;
        }
      }

      List<String> taskSessionIds =
          accessor.getChildNames(keyBuilder.taskCurrentStateSessions(participant));
      for (String sessionId : taskSessionIds) {
        CurrentState taskCurrentState = accessor
            .getProperty(keyBuilder.taskCurrentState(participant, sessionId, _resourceName));
        Map<String, String> taskPartitionStateMap = taskCurrentState.getPartitionStateMap();
        if (taskPartitionStateMap != null && !taskPartitionStateMap.isEmpty()) {
          LOG.error("Task current state not empty for " + participant);
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public ZkClient getZkClient() {
    return (ZkClient) _zkClient;
  }

  @Override
  public String getClusterName() {
    return _clusterName;
  }
}
