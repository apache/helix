package org.apache.helix.monitoring;

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

public class StateTransitionContext {
  private final String _resourceName;
  private final String _clusterName;
  private final String _instanceName;
  private final String _transition;

  public StateTransitionContext(String clusterName, String instanceName, String resourceName,
      String transition) {
    _clusterName = clusterName;
    _resourceName = resourceName;
    _transition = transition;
    _instanceName = instanceName;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public String getResourceName() {
    return _resourceName;
  }

  public String getTransition() {
    return _transition;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StateTransitionContext)) {
      return false;
    }

    StateTransitionContext otherCxt = (StateTransitionContext) other;
    return _clusterName.equals(otherCxt.getClusterName())
        &&
        // _instanceName.equals(otherCxt.getInstanceName()) &&
        _resourceName.equals(otherCxt.getResourceName())
        && _transition.equals(otherCxt.getTransition());
  }

  // In the report, we will gather per transition time statistics
  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  public String toString() {
    return "Cluster=" + _clusterName + "," +
    // "instance=" + _instanceName + "," +
        "Resource=" + _resourceName + "," + "Transition=" + _transition;
  }

}
