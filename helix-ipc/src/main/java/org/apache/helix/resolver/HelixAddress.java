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

import java.net.InetSocketAddress;

public class HelixAddress {

  private final HelixMessageScope scope;
  private final String instanceName;
  private final InetSocketAddress socketAddress;

  public HelixAddress(HelixMessageScope scope, String instanceName, InetSocketAddress socketAddress) {
    this.scope = scope;
    this.instanceName = instanceName;
    this.socketAddress = socketAddress;
  }

  public HelixMessageScope getScope() {
    return scope;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public InetSocketAddress getSocketAddress() {
    return socketAddress;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).addValue(scope).addValue(instanceName)
        .addValue(socketAddress).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(scope, instanceName, socketAddress);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HelixAddress)) {
      return false;
    }
    HelixAddress a = (HelixAddress) o;
    return a.getScope().equals(scope) && a.getInstanceName().equals(instanceName)
        && a.getSocketAddress().equals(socketAddress);
  }
}
