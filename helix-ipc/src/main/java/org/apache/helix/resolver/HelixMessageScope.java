package org.apache.helix.resolver;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.base.Objects;

/**
 * A definition of the addressing scope of a message.
 */
public class HelixMessageScope {
  private final String _cluster;
  private final String _resource;
  private final String _partition;
  private final String _state;
  private final String _srcInstance;

  private HelixMessageScope(String cluster, String resource, String partition, String state,
      String srcInstance) {
    _cluster = cluster;
    _resource = resource;
    _partition = partition;
    _state = state;
    _srcInstance = srcInstance;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).addValue(_cluster).addValue(_resource).addValue(_partition)
        .addValue(_state).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(_cluster, _resource, _partition, _state);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof HelixMessageScope) {
      HelixMessageScope that = (HelixMessageScope) other;
      return Objects.equal(_cluster, that._cluster) && Objects.equal(_resource, that._resource)
          && Objects.equal(_partition, that._partition) && Objects.equal(_state, that._state);
    }
    return false;
  }

  public String getCluster() {
    return _cluster;
  }

  public String getResource() {
    return _resource;
  }

  public String getPartition() {
    return _partition;
  }

  public String getState() {
    return _state;
  }

  public String getSourceInstance() {
    return _srcInstance;
  }

  public boolean isValid() {
    return _cluster != null && ((_partition != null && _resource != null) || _partition == null);
  }

  /**
   * Creator for a HelixMessageScope
   */
  public static class Builder {
    private String _cluster;
    private String _resource;
    private String _partition;
    private String _state;
    private String _sourceInstance;

    /**
     * Associate the scope with a cluster
     * @param cluster the cluster to scope routing to
     * @return Builder
     */
    public Builder cluster(String cluster) {
      _cluster = cluster;
      return this;
    }

    /**
     * Associate the scope with a resource
     * @param resource a resource served by the cluster
     * @return Builder
     */
    public Builder resource(String resource) {
      _resource = resource;
      return this;
    }

    /**
     * Associate the scope with a partition
     * @param partition a specific partition of the scoped resource
     * @return Builder
     */
    public Builder partition(String partition) {
      _partition = partition;
      return this;
    }

    /**
     * Associate the scope with a state
     * @param state a state that a resource in the cluster can be in
     * @return Builder
     */
    public Builder state(String state) {
      _state = state;
      return this;
    }

    public Builder sourceInstance(String sourceInstance) {
      _sourceInstance = sourceInstance;
      return this;
    }

    /**
     * Create the scope
     * @return HelixMessageScope instance corresponding to the built scope
     */
    public HelixMessageScope build() {
      return new HelixMessageScope(_cluster, _resource, _partition, _state, _sourceInstance);
    }
  }
}
