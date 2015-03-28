package org.apache.helix.ui;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;
import java.util.Set;

public class HelixUIApplicationConfiguration extends Configuration {
    @NotNull
    private ImmutableMap<String, ImmutableMap<String, String>> viewRendererConfiguration = ImmutableMap.of();

    private boolean adminMode = false;

    private Set<String> zkAddresses;

    @JsonProperty("viewRendererConfiguration")
    public ImmutableMap<String, ImmutableMap<String, String>> getViewRendererConfiguration() {
        return viewRendererConfiguration;
    }

    public boolean isAdminMode() {
        return adminMode;
    }

    public Set<String> getZkAddresses() {
        return zkAddresses;
    }
}
