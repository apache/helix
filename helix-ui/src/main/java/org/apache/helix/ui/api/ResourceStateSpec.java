package org.apache.helix.ui.api;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ResourceStateSpec {
    private final String resource;
    private final IdealState idealState;
    private final ExternalView externalView;
    private final Map<String, InstanceSpec> instanceSpecs;

    public ResourceStateSpec(String resource,
                             IdealState idealState,
                             ExternalView externalView,
                             Map<String, InstanceSpec> instanceSpecs) {
        this.resource = resource;
        this.idealState = idealState;
        this.externalView = externalView;
        this.instanceSpecs = instanceSpecs;
    }

    public String getResource() {
        return resource;
    }

    public IdealState getIdealState() {
        return idealState;
    }

    public ExternalView getExternalView() {
        return externalView;
    }

    public Map<String, InstanceSpec> getInstanceSpecs() {
        return instanceSpecs;
    }

    public List<ResourceStateTableRow> getResourceStateTable() {
        List<ResourceStateTableRow> resourceStateTable = new ArrayList<ResourceStateTableRow>();
        Set<String> partitionNames = idealState.getPartitionSet();
        for (String partitionName : partitionNames) {
            Map<String, String> stateMap = idealState.getInstanceStateMap(partitionName);
            if (stateMap != null) {
                for (Map.Entry<String, String> entry : stateMap.entrySet()) {
                    String instanceName = entry.getKey();
                    String ideal = entry.getValue();

                    String external = null;
                    if (externalView != null) {
                        Map<String, String> externalStateMap = externalView.getStateMap(partitionName);
                        if (externalStateMap != null) {
                            external = externalStateMap.get(instanceName);
                        }
                    }

                    resourceStateTable.add(new ResourceStateTableRow(resource, partitionName, instanceName, ideal, external));
                }
            }
        }

        return resourceStateTable;
    }
}
