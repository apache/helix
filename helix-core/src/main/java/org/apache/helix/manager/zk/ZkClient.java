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
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.*;
import org.apache.helix.monitoring.mbeans.ZkClientMonitor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * ZKClient does not provide some functionalities, this will be used for quick fixes if
 * any bug found in ZKClient or if we need additional features but can't wait for the new
 * ZkClient jar Ideally we should commit the changes we do here to ZKClient.
 */

public class ZkClient extends org.I0Itec.zkclient.ZkClient {
  private static Logger LOG = LoggerFactory.getLogger(ZkClient.class);
  public static final int DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;
  public static final int DEFAULT_SESSION_TIMEOUT = 30 * 1000;

  private PathBasedZkSerializer _zkSerializer;
  private ZkClientMonitor _monitor;

  private ZkClient(IZkConnection connection, int connectionTimeout, long operationRetryTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey,
      String monitorInstanceName, boolean monitorRootPathOnly) {
    super(connection, connectionTimeout, new ByteArraySerializer(), operationRetryTimeout);
    init(zkSerializer, monitorType, monitorKey, monitorInstanceName, monitorRootPathOnly);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey,
      long operationRetryTimeout) {
    this(connection, connectionTimeout, operationRetryTimeout, zkSerializer, monitorType,
        monitorKey, null, true);
  }

  public ZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey) {
    this(connection, connectionTimeout, zkSerializer, monitorType, monitorKey, -1);
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

  protected void init(PathBasedZkSerializer zkSerializer, String monitorType, String monitorKey,
      String monitorInstanceName, boolean monitorRootPathOnly) {
    _zkSerializer = zkSerializer;
    if (LOG.isTraceEnabled()) {
      StackTraceElement[] calls = Thread.currentThread().getStackTrace();
      LOG.trace("created a zkclient. callstack: " + Arrays.asList(calls));
    }
    try {
      if (monitorKey != null && !monitorKey.isEmpty() && monitorType != null && !monitorType
          .isEmpty()) {
        _monitor =
            new ZkClientMonitor(monitorType, monitorKey, monitorInstanceName, monitorRootPathOnly);
      } else {
        LOG.info("ZkClient monitor key or type is not provided. Skip monitoring.");
      }
    } catch (JMException e) {
      LOG.error("Error in creating ZkClientMonitor", e);
    }
  }

  @Override
  public void setZkSerializer(ZkSerializer zkSerializer) {
    _zkSerializer = new BasicZkSerializer(zkSerializer);
  }

  public void setZkSerializer(PathBasedZkSerializer zkSerializer) {
    _zkSerializer = zkSerializer;
  }

  public PathBasedZkSerializer getZkSerializer() {
    return _zkSerializer;
  }

  public IZkConnection getConnection() {
    return _connection;
  }

  @Override
  public void close() throws ZkInterruptedException {
    if (LOG.isTraceEnabled()) {
      StackTraceElement[] calls = Thread.currentThread().getStackTrace();
      LOG.trace("closing a zkclient. callStack: " + Arrays.asList(calls));
    }
    getEventLock().lock();
    try {
      if (_connection == null) {
        return;
      }
      LOG.info("Closing zkclient: " + ((ZkConnection) _connection).getZookeeper());
      super.close();
    } catch (ZkInterruptedException e) {
      /**
       * Workaround for HELIX-264: calling ZkClient#close() in its own eventThread context will
       * throw ZkInterruptedException and skip ZkConnection#close()
       */
      if (_connection != null) {
        try {
          /**
           * ZkInterruptedException#construct() honors InterruptedException by calling
           * Thread.currentThread().interrupt(); clear it first, so we can safely close the
           * zk-connection
           */
          Thread.interrupted();
          _connection.close();
          /**
           * restore interrupted status of current thread
           */
          Thread.currentThread().interrupt();
        } catch (InterruptedException e1) {
          throw new ZkInterruptedException(e1);
        }
      }
    } finally {
      getEventLock().unlock();
      if (_monitor != null) {
        _monitor.unregister();
      }
      LOG.info("Closed zkclient");
    }
  }

  public boolean isClosed() {
    return (_connection == null || !_connection.getZookeeperState().isAlive());
  }

  public Stat getStat(final String path) {
    long startT = System.currentTimeMillis();
    try {
      Stat stat = retryUntilConnected(new Callable<Stat>() {

        @Override
        public Stat call() throws Exception {
          Stat stat = ((ZkConnection) _connection).getZookeeper().exists(path, false);
          return stat;
        }
      });
      recordRead(path, null, startT);
      return stat;
    } catch (Exception e) {
      recordReadFailure(path);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  // override exists(path, watch), so we can record all exists requests
  @Override
  protected boolean exists(final String path, final boolean watch) {
    long startT = System.currentTimeMillis();
    try {
      boolean exists = retryUntilConnected(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return _connection.exists(path, watch);
        }
      });
      recordRead(path, null, startT);
      return exists;
    } catch (Exception e) {
      recordReadFailure(path);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  // override getChildren(path, watch), so we can record all getChildren requests
  @Override
  protected List<String> getChildren(final String path, final boolean watch) {
    long startT = System.currentTimeMillis();
    try {
      List<String> children = retryUntilConnected(new Callable<List<String>>() {
        @Override
        public List<String> call() throws Exception {
          return _connection.getChildren(path, watch);
        }
      });
      recordRead(path, null, startT);
      return children;
    } catch (Exception e) {
      recordReadFailure(path);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getChildren, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T deserialize(byte[] data, String path) {
    if (data == null) {
      return null;
    }
    return (T) _zkSerializer.deserialize(data, path);
  }

  // override readData(path, stat, watch), so we can record all read requests
  @Override
  @SuppressWarnings("unchecked")
  protected <T extends Object> T readData(final String path, final Stat stat, final boolean watch) {
    long startT = System.currentTimeMillis();
    byte[] data = null;
    try {
      data = retryUntilConnected(new Callable<byte[]>() {

        @Override
        public byte[] call() throws Exception {
          return _connection.readData(path, stat, watch);
        }
      });
      recordRead(path, data, startT);
      return (T) deserialize(data, path);
    } catch (Exception e) {
      recordReadFailure(path);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getData, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readDataAndStat(String path, Stat stat,
      boolean returnNullIfPathNotExists) {
    T data = null;
    try {
      data = readData(path, stat);
    } catch (ZkNoNodeException e) {
      if (!returnNullIfPathNotExists) {
        throw e;
      }
    }
    return data;
  }

  public String getServers() {
    return _connection.getServers();
  }

  public byte[] serialize(Object data, String path) {
    return _zkSerializer.serialize(data, path);
  }

  @Override
  public void writeData(final String path, Object datat, final int expectedVersion) {
    long startT = System.currentTimeMillis();
    try {
      final byte[] data = serialize(datat, path);
      checkDataSizeLimit(data);
      retryUntilConnected(new Callable<Object>() {

        @Override public Object call() throws Exception {
          _connection.writeData(path, data, expectedVersion);
          return null;
        }
      });
      recordWrite(path, data, startT);
    } catch (Exception e) {
      recordWriteFailure(path);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  public Stat writeDataGetStat(final String path, Object datat, final int expectedVersion)
      throws InterruptedException {
    long startT = System.currentTimeMillis();
    try {
      final byte[] data = _zkSerializer.serialize(datat, path);
      checkDataSizeLimit(data);
      Stat stat = retryUntilConnected(new Callable<Stat>() {

        @Override public Stat call() throws Exception {
          return ((ZkConnection) _connection).getZookeeper()
              .setData(path, data, expectedVersion);
        }
      });
      recordWrite(path, data, startT);
      return stat;
    } catch (Exception e) {
      recordWriteFailure(path);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  @Override
  public String create(final String path, Object datat, final CreateMode mode)
      throws IllegalArgumentException, ZkException {
    if (path == null) {
      throw new NullPointerException("path must not be null.");
    }
    long startT = System.currentTimeMillis();
    try {
      final byte[] data = datat == null ? null : serialize(datat, path);
      checkDataSizeLimit(data);
      String actualPath = retryUntilConnected(new Callable<String>() {
        @Override
        public String call() throws Exception {
          return _connection.create(path, data, mode);
        }
      });
      recordWrite(path, data, startT);
      return actualPath;
    } catch (Exception e) {
      recordWriteFailure(path);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("create, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
  }

  @Override
  public boolean delete(final String path) {
    long startT = System.currentTimeMillis();
    boolean isDeleted;
    try {
      try {
        retryUntilConnected(new Callable<Object>() {

          @Override
          public Object call() throws Exception {
            _connection.delete(path);
            return null;
          }
        });
        isDeleted = true;
      } catch (ZkNoNodeException e) {
        isDeleted = false;
        LOG.error("Failed to delete path " + path + ", znode does not exist!");
      }
      recordWrite(path, null, startT);
    } catch (Exception e) {
      recordWriteFailure(path);
      throw e;
    } finally {
      long endT = System.currentTimeMillis();
      if (LOG.isTraceEnabled()) {
        LOG.trace("delete, path: " + path + ", time: " + (endT - startT) + " ms");
      }
    }
    return isDeleted;
  }

  public void asyncCreate(final String path, Object datat, final CreateMode mode,
      final CreateCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    final byte[] data = (datat == null ? null : serialize(datat, path));
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().create(path, data, Ids.OPEN_ACL_UNSAFE,
            // Arrays.asList(DEFAULT_ACL),
            mode, cb, new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT,
                data == null ? 0 : data.length, false));
        return null;
      }
    });
  }

  public void asyncSetData(final String path, Object datat, final int version,
      final SetDataCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    final byte[] data = serialize(datat, path);
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().setData(path, data, version, cb,
            new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT,
                data == null ? 0 : data.length, false));
        return null;
      }
    });
  }

  public void asyncGetData(final String path, final GetDataCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().getData(path, null, cb,
            new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT, 0, true));
        return null;
      }
    });
  }

  public void asyncExists(final String path, final ExistsCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().exists(path, null, cb,
            new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT, 0, true));
        return null;
      }
    });
  }

  public void asyncDelete(final String path, final DeleteCallbackHandler cb) {
    final long startT = System.currentTimeMillis();
    retryUntilConnected(new Callable<Object>() {
      @Override public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().delete(path, -1, cb,
            new ZkAsyncCallbacks.ZkAsyncCallContext(_monitor, startT, 0, false));
        return null;
      }
    });
  }

  public <T> T retryUntilConnected(final Callable<T> callable) {
    final ZkConnection zkConnection = (ZkConnection) getConnection();
    return super.retryUntilConnected(new Callable<T>() {
      @Override
      public T call() throws Exception {
        // Validate that the connection is not null before trigger callback
        if (zkConnection == null || zkConnection.getZookeeper() == null) {
          throw new IllegalStateException(
              "ZkConnection is in invalid state! Please close this ZkClient and create new client.");
        }
        return callable.call();
      }
    });
  }

  private void checkDataSizeLimit(byte[] data) {
    if (data != null && data.length > ZNRecord.SIZE_LIMIT) {
      LOG.error("Data size larger than 1M, will not write to zk. Data (first 1k): "
          + new String(data).substring(0, 1024));
      throw new HelixException("Data size larger than 1M");
    }
  }

  @Override public void process(WatchedEvent event) {
    boolean stateChanged = event.getPath() == null;
    boolean dataChanged = event.getType() == Event.EventType.NodeDataChanged
        || event.getType() == Event.EventType.NodeDeleted
        || event.getType() == Event.EventType.NodeCreated
        || event.getType() == Event.EventType.NodeChildrenChanged;

    if (_monitor != null) {
      if (stateChanged) {
        _monitor.increaseStateChangeEventCounter();
      }
      if (dataChanged) {
        _monitor.increaseDataChangeEventCounter();
      }
    }

    super.process(event);
  }

  private void recordRead(String path, byte[] data, long startTimeMilliSec) {
    if (_monitor != null) {
      int dataSize = 0;
      if (data != null) {
        dataSize = data.length;
      }
      _monitor.recordRead(path, dataSize, startTimeMilliSec);
    }
  }

  private void recordWrite(String path, byte[] data, long startTimeMilliSec) {
    if (_monitor != null) {
      int dataSize = 0;
      if (data != null) {
        dataSize = data.length;
      }
      _monitor.recordWrite(path, dataSize, startTimeMilliSec);
    }
  }

  private void recordReadFailure(String path) {
    if (_monitor != null) {
      _monitor.recordReadFailure(path);
    }
  }

  private void recordWriteFailure(String path) {
    if (_monitor != null) {
      _monitor.recordWriteFailure(path);
    }
  }

  public static class Builder {
    IZkConnection _connection;
    String _zkServer;

    PathBasedZkSerializer _zkSerializer;

    long _operationRetryTimeout = -1L;
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
