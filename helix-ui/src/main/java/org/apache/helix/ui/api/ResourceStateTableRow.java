package org.apache.helix.ui.api;

public class ResourceStateTableRow implements Comparable<ResourceStateTableRow> {
    private static final String NA = "N/A";

    private final String resourceName;
    private final String partitionName;
    private final String instanceName;
    private final String ideal;
    private final String external;

    public ResourceStateTableRow(String resourceName,
                                 String partitionName,
                                 String instanceName,
                                 String ideal,
                                 String external) {
        this.resourceName = resourceName;
        this.partitionName = partitionName;
        this.instanceName = instanceName;
        this.ideal = ideal;
        this.external = external == null ? NA : external;
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getIdeal() {
        return ideal;
    }

    public String getExternal() {
        return external;
    }

    @Override
    public int compareTo(ResourceStateTableRow r) {
        int partitionResult = partitionName.compareTo(r.getPartitionName());
        if (partitionResult != 0) {
            return partitionResult;
        }

        int instanceResult = instanceName.compareTo(r.getInstanceName());
        if (instanceResult != 0) {
            return instanceResult;
        }

        int idealResult = ideal.compareTo(r.getIdeal());
        if (idealResult != 0) {
            return idealResult;
        }

        int externalResult = external.compareTo(r.getExternal());
        if (externalResult != 0) {
            return externalResult;
        }

        return 0;

    }
}
