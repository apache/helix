package org.apache.helix.api.status;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;

/**
 * Represents a request to set the cluster management mode {@link ClusterManagementMode}
 */
@JsonDeserialize(builder = ClusterManagementModeRequest.Builder.class)
public class ClusterManagementModeRequest {
  private final ClusterManagementMode.Type _mode;
  private final String _clusterName;
  private final String _reason;
  private final boolean _cancelPendingST;

  public ClusterManagementMode.Type getMode() {
    return _mode;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public String getReason() {
    return _reason;
  }

  public boolean isCancelPendingST() {
    return _cancelPendingST;
  }

  public ClusterManagementModeRequest(Builder builder) {
    _mode = builder.mode;
    _clusterName = builder.clusterName;
    _reason = builder.reason;
    _cancelPendingST = builder.cancelPendingST;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "buildFromJson")
  public static final class Builder {
    private ClusterManagementMode.Type mode;
    private String clusterName;
    private String reason = "";
    private boolean cancelPendingST;

    public Builder withMode(ClusterManagementMode.Type mode) {
      this.mode = mode;
      return this;
    }

    public Builder withClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder withReason(String reason) {
      this.reason = reason;
      return this;
    }

    /**
     * If mode is not CLUSTER_PAUSE, this should not be set to true.
     *
     * @param cancelPendingST whether or not cancel pending ST for CLUSTER_PAUSE mode.
     * @return {@link Builder}
     */
    public Builder withCancelPendingST(boolean cancelPendingST) {
      this.cancelPendingST = cancelPendingST;
      return this;
    }

    public ClusterManagementModeRequest build() {
      validate();
      return new ClusterManagementModeRequest(this);
    }

    // Used by Json deserializer
    private ClusterManagementModeRequest buildFromJson() {
      Preconditions.checkNotNull(mode, "Mode not set");
      return new ClusterManagementModeRequest((this));
    }

    private void validate() {
      Preconditions.checkNotNull(mode, "Mode not set");
      Preconditions.checkNotNull(clusterName, "Cluster name not set");
    }
  }
}
