package org.apache.helix.resolver.zk;

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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.resolver.AbstractHelixResolver;

/**
 * A ZooKeeper-specific {@link org.apache.helix.resolver.HelixResolver}
 */
public class ZKHelixResolver extends AbstractHelixResolver {
  private final String _zkAddress;

  /**
   * Create a ZK-based Helix resolver
   * @param zkConnectString the connection string to the ZooKeeper ensemble
   */
  public ZKHelixResolver(String zkConnectString) {
    _zkAddress = zkConnectString;
  }

  @Override
  protected HelixManager createManager(String cluster) {
    return HelixManagerFactory.getZKHelixManager(cluster, null, InstanceType.SPECTATOR, _zkAddress);
  }
}
