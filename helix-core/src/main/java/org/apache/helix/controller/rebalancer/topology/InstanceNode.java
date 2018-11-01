package org.apache.helix.controller.rebalancer.topology;

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

public class InstanceNode extends Node {
  private String _instanceName;

  public InstanceNode(Node node, String instanceName) {
    super(node);
    _instanceName = instanceName;
  }

  public InstanceNode clone() {
    return new InstanceNode(this, _instanceName);
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public void setInstanceName(String instanceName) {
    _instanceName = instanceName;
  }

  @Override
  public String toString() {
    return super.toString() + ":" + _instanceName;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof InstanceNode)) {
      return false;
    }
    InstanceNode that = (InstanceNode)obj;
    return super.equals(that) && _instanceName.equals(that.getInstanceName());
  }

  /**
   * Do not override compareTo & hashCode. This are used to determine node position in the topology.
   * Instance name should not affect the topology location in anyway.
   */
}
