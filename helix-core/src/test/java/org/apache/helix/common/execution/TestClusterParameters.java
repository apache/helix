package org.apache.helix.common.execution;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.helix.model.IdealState.RebalanceMode;

import static org.apache.commons.lang3.RandomStringUtils.*;
import static org.apache.helix.TestHelper.*;


public class TestClusterParameters {

  private final String _clusterName;
  private final String _resourceName;
  private final String _stateModelDef;
  private RebalanceMode _rebalanceMode;
  private final int _resourcesCount;
  private final int _partitionsPerResource;
  private final int _nodeCount;
  private final int _replicaCount;
  private final boolean _isRebalanced;

  private TestClusterParameters(
      String clusterName, String resourceName, String stateModelDef, RebalanceMode rebalanceMode, int resourcesCount,
      int partitionsPerResource, int nodeCount, int replicaCount, boolean isRebalanced) {
    _clusterName = clusterName;
    _resourceName = resourceName;
    _stateModelDef = stateModelDef;
    _rebalanceMode = rebalanceMode;
    _resourcesCount = resourcesCount;
    _partitionsPerResource = partitionsPerResource;
    _nodeCount = nodeCount;
    _replicaCount = replicaCount;
    _isRebalanced = isRebalanced;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public String getResourceName() {
    return _resourceName;
  }

  public String getStateModelDef() {
    return _stateModelDef;
  }

  public int getResourcesCount() {
    return _resourcesCount;
  }

  public int getPartitionsPerResource() {
    return _partitionsPerResource;
  }

  public int getNodeCount() {
    return _nodeCount;
  }

  public int getReplicaCount() {
    return _replicaCount;
  }

  public boolean isRebalanced() {
    return _isRebalanced;
  }

  /**
   * Builder Class for {@link TestClusterParameters}
   */
  public static class Builder {
    private String _clusterName;
    private String _resourceName;
    private String _stateModelDef;
    private RebalanceMode _rebalanceMode;
    private int _resourcesCount = 1;
    private int _partitionsPerResource = 1;
    private int _nodeCount = 1;
    private int _replicaCount = 1;
    private boolean _isRebalanced = false;

    public Builder() {
    }

    public TestClusterParameters build() {
      _clusterName = ObjectUtils.defaultIfNull(_clusterName,
          String.format("%s.%s.%s", getTestClassName(), getTestClassName(), randomAlphabetic(10)));

      return new TestClusterParameters(_clusterName, _resourceName, _stateModelDef, _rebalanceMode, _resourcesCount,
          _partitionsPerResource, _nodeCount, _replicaCount, _isRebalanced);
    }

    public Builder setClusterName(String clusterName) {
      _clusterName = clusterName;
      return this;
    }

    public Builder setResourceName(String resourceName) {
      _resourceName = resourceName;
      return this;
    }

    public Builder setResourcesCount(int resourcesCount) {
      _resourcesCount = resourcesCount;
      return this;
    }

    public Builder setRebalanceMode(RebalanceMode rebalanceMode) {
      _rebalanceMode = rebalanceMode;
      return this;
    }

    public Builder setPartitionsPerResource(int partitionsPerResource) {
      _partitionsPerResource = partitionsPerResource;
      return this;
    }

    public Builder setNodeCount(int nodeCount) {
      _nodeCount = nodeCount;
      return this;
    }

    public Builder setReplicaCount(int replicaCount) {
      _replicaCount = replicaCount;
      return this;
    }

    public Builder withMasterSlave() {
      _stateModelDef = "MasterSlave";
      return this;
    }

    public Builder setStateModelDef(String stateModelDef) {
      _stateModelDef = stateModelDef;
      return this;
    }

    public Builder enableRebalance() {
      _isRebalanced = true;
      return this;
    }

    public Builder disableRebalance() {
      _isRebalanced = false;
      return this;
    }
  }

}
