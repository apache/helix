package org.apache.helix.manager.zk;

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

import org.apache.helix.zookeeper.zkclient.IZkConnection;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;


/**
 * Do NOT use; deprecated and replaced by org.apache.helix.zookeeper.impl.client.ZkClient.
 */
@Deprecated
public class ZkClient extends org.apache.helix.zookeeper.impl.client.ZkClient {
  /**
   *  @param zkConnection
   *            The Zookeeper connection
   * @param connectionTimeout
   *            The connection timeout in milli seconds
   * @param operationRetryTimeout
   *            Most operations are retried in cases like connection loss with the Zookeeper servers. During such failures, this
   *            <code>operationRetryTimeout</code> decides the maximum amount of time, in milli seconds, each
   *            operation is retried. A value lesser than 0 is considered as
   *            "retry forever until a connection has been reestablished".
   * @param zkSerializer
   *            The Zookeeper data serializer
   * @param monitorType
   * @param monitorKey
   * @param monitorInstanceName
   *            These 3 inputs are used to name JMX monitor bean name for this RealmAwareZkClient.
   *            The JMX bean name will be: HelixZkClient.monitorType.monitorKey.monitorInstanceName.
   * @param monitorRootPathOnly
   */
  public ZkClient(IZkConnection zkConnection, int connectionTimeout, long operationRetryTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey,
      String monitorInstanceName, boolean monitorRootPathOnly) {
    super(zkConnection, connectionTimeout, operationRetryTimeout, zkSerializer, monitorType,
        monitorKey, monitorInstanceName, monitorRootPathOnly);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey,
      long operationRetryTimeout) {
    super(connection, connectionTimeout, zkSerializer, monitorType, monitorKey,
        operationRetryTimeout);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey) {
    super(connection, connectionTimeout, zkSerializer, monitorType, monitorKey);
  }

  public ZkClient(String zkServers, String monitorType, String monitorKey) {
    super(zkServers, monitorType, monitorKey);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey) {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer, monitorType, monitorKey);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer) {
    super(connection, connectionTimeout, zkSerializer);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout, ZkSerializer zkSerializer) {
    super(connection, connectionTimeout, zkSerializer);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout) {
    super(connection, connectionTimeout);
  }

  public ZkClient(IZkConnection connection) {
    super(connection);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      ZkSerializer zkSerializer) {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      PathBasedZkSerializer zkSerializer) {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
    super(zkServers, sessionTimeout, connectionTimeout);
  }

  public ZkClient(String zkServers, int connectionTimeout) {
    super(zkServers, connectionTimeout);
  }

  public ZkClient(String zkServers) {
    super(zkServers);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      ZkSerializer zkSerializer, long operationRetryTimeout) {
    super(zkServers, sessionTimeout, connectionTimeout, zkSerializer, operationRetryTimeout);
  }

  public ZkClient(IZkConnection zkConnection, int connectionTimeout, ZkSerializer zkSerializer,
      long operationRetryTimeout) {
    super(zkConnection, connectionTimeout, zkSerializer, operationRetryTimeout);
  }
}
