package org.apache.helix.ui.api;

public class ResourceSpec extends ClusterSpec {
    private final String resourceName;

    public ResourceSpec(String zkAddress, String clusterName, String resourceName) {
        super(zkAddress, clusterName);
        this.resourceName = resourceName;
    }

    public String getResourceName() {
        return resourceName;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ResourceSpec)) {
            return false;
        }
        ResourceSpec c = (ResourceSpec) o;
        return getZkAddress().equals(c.getZkAddress())
                && getClusterName().equals(c.getClusterName())
                && resourceName.equals(c.getResourceName());
    }

    @Override
    public int hashCode() {
        return super.hashCode() + 27 * resourceName.hashCode();
    }

}
