package org.apache.helix.ui.api;

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

public class ClusterSpec {
  private final String zkAddress;
  private final String clusterName;

  public ClusterSpec(String zkAddress, String clusterName) {
    this.zkAddress = zkAddress;
    this.clusterName = clusterName;
  }

  public String getZkAddress() {
    return zkAddress;
  }

  public String getClusterName() {
    return clusterName;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ClusterSpec)) {
      return false;
    }
    ClusterSpec c = (ClusterSpec) o;
    return c.zkAddress.equals(zkAddress) && c.clusterName.equals(clusterName);
  }

  @Override
  public int hashCode() {
    return zkAddress.hashCode() + 13 * clusterName.hashCode();
  }
}
