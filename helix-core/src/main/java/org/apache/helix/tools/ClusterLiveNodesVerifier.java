package org.apache.helix.tools;

import java.util.Collections;
import java.util.List;

import org.apache.helix.manager.zk.ZkClient;

public class ClusterLiveNodesVerifier extends ClusterVerifier {

  final List<String> _expectSortedLiveNodes; // always sorted

  public ClusterLiveNodesVerifier(ZkClient zkclient, String clusterName,
      List<String> expectLiveNodes) {
    super(zkclient, clusterName);
    _expectSortedLiveNodes = expectLiveNodes;
    Collections.sort(_expectSortedLiveNodes);
  }

  @Override
  public boolean verify() throws Exception {
    List<String> actualLiveNodes = _accessor.getChildNames(_keyBuilder.liveInstances());
    Collections.sort(actualLiveNodes);
    return _expectSortedLiveNodes.equals(actualLiveNodes);
  }

}
