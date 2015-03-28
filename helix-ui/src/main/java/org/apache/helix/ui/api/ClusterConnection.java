package org.apache.helix.ui.api;

import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.tools.ClusterSetup;

public class ClusterConnection {
    private final ZkClient zkClient;
    private final ClusterSetup clusterSetup;

    public ClusterConnection(ZkClient zkClient) {
        this.zkClient = zkClient;
        this.clusterSetup = new ClusterSetup(zkClient);
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public ClusterSetup getClusterSetup() {
        return clusterSetup;
    }
}
