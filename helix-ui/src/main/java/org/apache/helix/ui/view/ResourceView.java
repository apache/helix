package org.apache.helix.ui.view;

import io.dropwizard.views.View;
import org.apache.helix.ui.api.ConfigTableRow;
import org.apache.helix.ui.api.IdealStateSpec;
import org.apache.helix.ui.api.InstanceSpec;
import org.apache.helix.ui.api.ResourceStateTableRow;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ResourceView extends View {
    private final boolean adminMode;
    private final String zkAddress;
    private final List<String> clusters;
    private final boolean activeValid;
    private final String activeCluster;
    private final List<String> activeClusterResources;
    private final String activeResource;
    private final List<ResourceStateTableRow> resourceStateTable;
    private final List<ConfigTableRow> configTable;
    private final IdealStateSpec idealStateSpec;
    private final List<InstanceSpec> instanceSpecs;

    public ResourceView(boolean adminMode,
                        String zkAddress,
                        List<String> clusters,
                        boolean activeValid,
                        String activeCluster,
                        List<String> activeClusterResources,
                        String activeResource,
                        List<ResourceStateTableRow> resourceStateTable,
                        Set<String> resourceInstances,
                        List<ConfigTableRow> configTable,
                        IdealStateSpec idealStateSpec,
                        List<InstanceSpec> instanceSpecs) {
        super("resource-view.ftl");
        this.adminMode = adminMode;
        this.zkAddress = zkAddress;
        this.clusters = clusters;
        this.activeValid = activeValid;
        this.activeCluster = activeCluster;
        this.activeClusterResources = activeClusterResources;
        this.activeResource = activeResource;
        this.resourceStateTable = resourceStateTable;
        this.configTable = configTable;
        this.idealStateSpec = idealStateSpec;
        this.instanceSpecs = new ArrayList<InstanceSpec>();

        for (InstanceSpec instanceSpec : instanceSpecs) {
            if (resourceInstances.contains(instanceSpec.getInstanceName())) {
                this.instanceSpecs.add(instanceSpec);
            }
        }
    }

    public boolean isAdminMode() {
        return adminMode;
    }

    public String getZkAddress() throws IOException {
        return URLEncoder.encode(zkAddress, "UTF-8");
    }

    public List<String> getClusters() {
        return clusters;
    }

    public boolean isActiveValid() {
        return activeValid;
    }

    public String getActiveCluster() {
        return activeCluster;
    }

    public List<String> getActiveClusterResources() {
        return activeClusterResources;
    }

    public List<InstanceSpec> getInstanceSpecs() {
        return instanceSpecs;
    }

    public String getActiveResource() {
        return activeResource;
    }

    public List<ResourceStateTableRow> getResourceStateTable() {
        return resourceStateTable;
    }

    public List<ConfigTableRow> getConfigTable() {
        return configTable;
    }

    public IdealStateSpec getIdealStateSpec() {
        return idealStateSpec;
    }
}
