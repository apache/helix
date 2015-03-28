package org.apache.helix.ui.api;

public class ClusterSpec {
    private final String zkAddress;
    private final String clusterName;

    public ClusterSpec(String zkAddress, String clusterName) {
        this.zkAddress = zkAddress;
        this.clusterName = clusterName;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public String getClusterName() {
        return clusterName;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ClusterSpec)) {
            return false;
        }
        ClusterSpec c = (ClusterSpec) o;
        return c.zkAddress.equals(zkAddress) && c.clusterName.equals(clusterName);
    }

    @Override
    public int hashCode() {
        return zkAddress.hashCode() + 13 * clusterName.hashCode();
    }
}
