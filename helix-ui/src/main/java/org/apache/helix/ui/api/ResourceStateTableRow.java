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

public class ResourceStateTableRow implements Comparable<ResourceStateTableRow> {
  private static final String NA = "N/A";

  private final String resourceName;
  private final String partitionName;
  private final String instanceName;
  private final String ideal;
  private final String external;

  public ResourceStateTableRow(String resourceName,
                               String partitionName,
                               String instanceName,
                               String ideal,
                               String external) {
    this.resourceName = resourceName;
    this.partitionName = partitionName;
    this.instanceName = instanceName;
    this.ideal = ideal;
    this.external = external == null ? NA : external;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getIdeal() {
    return ideal;
  }

  public String getExternal() {
    return external;
  }

  @Override
  public int compareTo(ResourceStateTableRow r) {
    int partitionResult = partitionName.compareTo(r.getPartitionName());
    if (partitionResult != 0) {
      return partitionResult;
    }

    int instanceResult = instanceName.compareTo(r.getInstanceName());
    if (instanceResult != 0) {
      return instanceResult;
    }

    int idealResult = ideal.compareTo(r.getIdeal());
    if (idealResult != 0) {
      return idealResult;
    }

    int externalResult = external.compareTo(r.getExternal());
    if (externalResult != 0) {
      return externalResult;
    }

    return 0;

  }
}
