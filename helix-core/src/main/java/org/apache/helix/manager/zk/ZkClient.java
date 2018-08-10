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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raw ZkClient that wraps {@link org.apache.helix.manager.zk.zookeeper.ZkClient},
 * with additional constructors and builder.
 *
 * Note that, instead of directly constructing a raw ZkClient, applications should always use
 * HelixZkClientFactory to build shared or dedicated HelixZkClient instances.
 * Only constructing a raw ZkClient when advanced usage is required.
 * For example, application need to access/manage ZkConnection directly.
 *
 * Both SharedZKClient and DedicatedZkClient are built based on the raw ZkClient. As shown below.
 *                ----------------------------
 *               |                            |
 *     ---------------------                  |
 *    |                     |                 | *implements
 *  SharedZkClient  DedicatedZkClient           ----> HelixZkClient Interface
 *    |                     |                 |
 *     ---------------------                  |
 *               |                            |
 *           Raw ZkClient (this class)--------
 *               |
 *         Native ZkClient
 *
 * TODO Completely replace usage of the raw ZkClient within helix-core. Instead, using HelixZkClient. --JJ
 */

public class ZkClient extends org.apache.helix.manager.zk.zookeeper.ZkClient implements HelixZkClient {
  private static Logger LOG = LoggerFactory.getLogger(ZkClient.class);

  public static final int DEFAULT_OPERATION_TIMEOUT = Integer.MAX_VALUE;
  public static final int DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;
  public static final int DEFAULT_SESSION_TIMEOUT = 30 * 1000;

  /**
   *
   * @param zkConnection
   *            The Zookeeper connection
   * @param connectionTimeout
   *            The connection timeout in milli seconds
   * @param zkSerializer
   *            The Zookeeper data serializer
   * @param operationRetryTimeout
   *            Most operations are retried in cases like connection loss with the Zookeeper servers. During such failures, this
   *            <code>operationRetryTimeout</code> decides the maximum amount of time, in milli seconds, each
   *            operation is retried. A value lesser than 0 is considered as
   *            "retry forever until a connection has been reestablished".
   * @param monitorType
   * @param monitorKey
   * @param monitorInstanceName
   *            These 3 inputs are used to name JMX monitor bean name for this ZkClient.
   *            The JMX bean name will be: HelixZkClient.monitorType.monitorKey.monitorInstanceName.
   * @param monitorRootPathOnly
   *            Should only stat of access to root path be reported to JMX bean or path-specific stat be reported too.
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
    this(connection, connectionTimeout, operationRetryTimeout, zkSerializer, monitorType,
        monitorKey, null, true);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey) {
    this(connection, connectionTimeout, zkSerializer, monitorType, monitorKey, DEFAULT_OPERATION_TIMEOUT);
  }

  public ZkClient(String zkServers, String monitorType, String monitorKey) {
    this(new ZkConnection(zkServers, DEFAULT_SESSION_TIMEOUT), Integer.MAX_VALUE,
        new BasicZkSerializer(new SerializableSerializer()), monitorType, monitorKey);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey) {
    this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer, monitorType,
        monitorKey);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer) {
    this(connection, connectionTimeout, zkSerializer, null, null);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout, ZkSerializer zkSerializer) {
    this(connection, connectionTimeout, new BasicZkSerializer(zkSerializer));
  }

  public ZkClient(IZkConnection connection, int connectionTimeout) {
    this(connection, connectionTimeout, new SerializableSerializer());
  }

  public ZkClient(IZkConnection connection) {
    this(connection, Integer.MAX_VALUE, new SerializableSerializer());
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      ZkSerializer zkSerializer) {
    this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      PathBasedZkSerializer zkSerializer) {
    this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
    this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout,
        new SerializableSerializer());
  }

  public ZkClient(String zkServers, int connectionTimeout) {
    this(new ZkConnection(zkServers, DEFAULT_SESSION_TIMEOUT), connectionTimeout,
        new SerializableSerializer());
  }

  public ZkClient(String zkServers) {
    this(zkServers, null, null);
  }

  public ZkClient(final String zkServers, final int sessionTimeout, final int connectionTimeout,
      final ZkSerializer zkSerializer, final long operationRetryTimeout) {
    this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer,
        operationRetryTimeout);
  }

  public ZkClient(final IZkConnection zkConnection, final int connectionTimeout,
      final ZkSerializer zkSerializer, final long operationRetryTimeout) {
    this(zkConnection, connectionTimeout, operationRetryTimeout,
        new BasicZkSerializer(zkSerializer), null, null, null, false);
  }

  public static class Builder {
    IZkConnection _connection;
    String _zkServer;

    PathBasedZkSerializer _zkSerializer;

    long _operationRetryTimeout = DEFAULT_OPERATION_TIMEOUT;
    int _connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
    int _sessionTimeout = DEFAULT_SESSION_TIMEOUT;

    String _monitorType;
    String _monitorKey;
    String _monitorInstanceName = null;
    boolean _monitorRootPathOnly = true;

    public Builder setConnection(IZkConnection connection) {
      this._connection = connection;
      return this;
    }

    public Builder setConnectionTimeout(Integer connectionTimeout) {
      this._connectionTimeout = connectionTimeout;
      return this;
    }

    public Builder setZkSerializer(PathBasedZkSerializer zkSerializer) {
      this._zkSerializer = zkSerializer;
      return this;
    }

    public Builder setZkSerializer(ZkSerializer zkSerializer) {
      this._zkSerializer = new BasicZkSerializer(zkSerializer);
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     * @param monitorType
     */
    public Builder setMonitorType(String monitorType) {
      this._monitorType = monitorType;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     * @param monitorKey
     */
    public Builder setMonitorKey(String monitorKey) {
      this._monitorKey = monitorKey;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is optional.
     * @param instanceName
     */
    public Builder setMonitorInstanceName(String instanceName) {
      this._monitorInstanceName = instanceName;
      return this;
    }


    public Builder setMonitorRootPathOnly(Boolean monitorRootPathOnly) {
      this._monitorRootPathOnly = monitorRootPathOnly;
      return this;
    }

    public Builder setZkServer(String zkServer) {
      this._zkServer = zkServer;
      return this;
    }

    public Builder setSessionTimeout(Integer sessionTimeout) {
      this._sessionTimeout = sessionTimeout;
      return this;
    }

    public Builder setOperationRetryTimeout(Long operationRetryTimeout) {
      this._operationRetryTimeout = operationRetryTimeout;
      return this;
    }

    public ZkClient build() {
      if (_connection == null) {
        if (_zkServer == null) {
          throw new HelixException(
              "Failed to build ZkClient since no connection or ZK server address is specified.");
        } else {
          _connection = new ZkConnection(_zkServer, _sessionTimeout);
        }
      }

      if (_zkSerializer == null) {
        _zkSerializer = new BasicZkSerializer(new SerializableSerializer());
      }

      return new ZkClient(_connection, _connectionTimeout, _operationRetryTimeout, _zkSerializer,
          _monitorType, _monitorKey, _monitorInstanceName, _monitorRootPathOnly);
    }
  }
}
