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

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class D3ResourceCoCentricCircle {

  public enum CircleType {
    CLUSTER,
    INSTANCE,
    PARTITION
  }

  private final String name;
  private final String parentName;
  private final String state;
  private final int size;
  private final CircleType circleType;
  private final Set<D3ResourceCoCentricCircle> children;

  public D3ResourceCoCentricCircle(String name,
                                   String parentName,
                                   String state,
                                   int size,
                                   CircleType circleType,
                                   Set<D3ResourceCoCentricCircle> children) {
    this.name = name;
    this.parentName = parentName;
    this.state = state;
    this.size = size;
    this.circleType = circleType;
    this.children = children;
  }

  public String getName() {
    return name;
  }

  public String getParentName() {
    return parentName;
  }

  public String getState() {
    return state;
  }

  public int getSize() {
    return size;
  }

  public CircleType getCircleType() {
    return circleType;
  }

  public Set<D3ResourceCoCentricCircle> getChildren() {
    return children;
  }

  public static D3ResourceCoCentricCircle fromResourceStateSpec(ResourceStateSpec resourceStateSpec) {
    Map<String, Set<D3ResourceCoCentricCircle>> partitionByInstance
            = new HashMap<String, Set<D3ResourceCoCentricCircle>>();

    // Group by instance (first level)
    for (ResourceStateTableRow row : resourceStateSpec.getResourceStateTable()) {
      Set<D3ResourceCoCentricCircle> partitionCircles = partitionByInstance.get(row.getInstanceName());
      if (partitionCircles == null) {
        partitionCircles = new HashSet<D3ResourceCoCentricCircle>();
        partitionByInstance.put(row.getInstanceName(), partitionCircles);
      }
      partitionCircles.add(new D3ResourceCoCentricCircle(
              row.getPartitionName(),
              row.getInstanceName(),
              row.getExternal(),
              10,
              CircleType.PARTITION,
              null));
    }

    // Group into cluster
    Set<D3ResourceCoCentricCircle> instanceCircles = new HashSet<D3ResourceCoCentricCircle>();
    for (Map.Entry<String, Set<D3ResourceCoCentricCircle>> entry : partitionByInstance.entrySet()) {

      InstanceSpec instanceSpec = resourceStateSpec.getInstanceSpecs().get(entry.getKey());
      if (instanceSpec == null) {
        throw new IllegalStateException("No instance spec for " + entry.getKey());
      }

      String state;
      if (!instanceSpec.isLive()) {
        state = "DEAD";
      } else if (!instanceSpec.isEnabled()) {
        state = "DISABLED";
      } else {
        state = "LIVE";
      }

      instanceCircles.add(new D3ResourceCoCentricCircle(
              entry.getKey(),
              resourceStateSpec.getResource(),
              state,
              100,
              CircleType.INSTANCE,
              entry.getValue()));
    }

    return new D3ResourceCoCentricCircle(
            resourceStateSpec.getIdealState().getResourceName(),
            null,
            "",
            900,
            CircleType.CLUSTER,
            instanceCircles);
  }
}
