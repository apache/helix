package org.apache.helix.ui.api;

public class InstanceSpec implements Comparable<InstanceSpec> {
    private final String instanceName;
    private final boolean enabled;
    private final boolean live;

    public InstanceSpec(String instanceName,
                        boolean enabled,
                        boolean live) {
        this.instanceName = instanceName;
        this.enabled = enabled;
        this.live = live;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isLive() {
        return live;
    }

    @Override
    public int compareTo(InstanceSpec o) {
        return instanceName.compareTo(o.instanceName);
    }
}
