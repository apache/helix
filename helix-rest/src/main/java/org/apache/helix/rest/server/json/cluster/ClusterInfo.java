package org.apache.helix.rest.server.json.cluster;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


public class ClusterInfo {
  @JsonProperty("id")
  private final String id;
  @JsonProperty("controller")
  private final String controller;
  @JsonProperty("paused")
  private final boolean paused;
  @JsonProperty("maintenance")
  private final boolean maintenance;
  @JsonProperty("resources")
  private final List<String> idealStates;
  @JsonProperty("instances")
  private final List<String> instances;
  @JsonProperty("liveInstances")
  private final List<String> liveInstances;

  private ClusterInfo(Builder builder) {
    id = builder.id;
    controller = builder.controller;
    paused = builder.paused;
    maintenance = builder.maintenance;
    idealStates = builder.idealStates;
    instances = builder.instances;
    liveInstances = builder.liveInstances;
  }

  public static final class Builder {
    private String id;
    private String controller;
    private boolean paused;
    private boolean maintenance;
    private List<String> idealStates;
    private List<String> instances;
    private List<String> liveInstances;

    public Builder(String id) {
      this.id = id;
    }

    public Builder controller(String controller) {
      this.controller = controller;
      return this;
    }

    public Builder paused(boolean paused) {
      this.paused = paused;
      return this;
    }

    public Builder maintenance(boolean maintenance) {
      this.maintenance = maintenance;
      return this;
    }

    public Builder idealStates(List<String> idealStates) {
      this.idealStates = idealStates;
      return this;
    }

    public Builder instances(List<String> instances) {
      this.instances = instances;
      return this;
    }

    public Builder liveInstances(List<String> liveInstances) {
      this.liveInstances = liveInstances;
      return this;
    }

    public ClusterInfo build() {
      return new ClusterInfo(this);
    }
  }
}
