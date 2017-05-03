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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.CreateCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.DeleteCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.ExistsCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.GetDataCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.SetDataCallbackHandler;
import org.apache.helix.monitoring.mbeans.ZkClientMonitor;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import javax.management.JMException;

/**
 * ZKClient does not provide some functionalities, this will be used for quick fixes if
 * any bug found in ZKClient or if we need additional features but can't wait for the new
 * ZkClient jar Ideally we should commit the changes we do here to ZKClient.
 */

public class ZkClient extends org.I0Itec.zkclient.ZkClient {
  private static Logger LOG = Logger.getLogger(ZkClient.class);
  public static final int DEFAULT_CONNECTION_TIMEOUT = 60 * 1000;
  public static final int DEFAULT_SESSION_TIMEOUT = 30 * 1000;
  // public static String sessionId;
  // public static String sessionPassword;

  private PathBasedZkSerializer _zkSerializer;
  private ZkClientMonitor _monitor;

  public ZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorTag) {
    super(connection, connectionTimeout, new ByteArraySerializer());
    _zkSerializer = zkSerializer;
    if (LOG.isTraceEnabled()) {
      StackTraceElement[] calls = Thread.currentThread().getStackTrace();
      LOG.trace("created a zkclient. callstack: " + Arrays.asList(calls));
    }
    try {
      if (monitorTag == null) {
        _monitor = new ZkClientMonitor();
      } else {
        _monitor = new ZkClientMonitor(monitorTag);
      }
    } catch (JMException e) {
      LOG.error("Error in creating ZkClientMonitor", e);
    }
  }

  public ZkClient(IZkConnection connection, int connectionTimeout,
      PathBasedZkSerializer zkSerializer) {
    this(connection, connectionTimeout, zkSerializer, null);
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

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout,
      PathBasedZkSerializer zkSerializer, String monitorTag) {
    this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer, monitorTag);
  }

  public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
    this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout,
        new SerializableSerializer());
  }

  public ZkClient(String zkServers, int connectionTimeout) {
    this(new ZkConnection(zkServers), connectionTimeout, new SerializableSerializer());
  }

  public ZkClient(String zkServers) {
    this(zkServers, null);
  }

  public ZkClient(String zkServers, String monitorTag) {
    this(new ZkConnection(zkServers), Integer.MAX_VALUE,
        new BasicZkSerializer(new SerializableSerializer()), monitorTag);
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
      _monitor.unregister();
      LOG.info("Closed zkclient");
    }
  }

  public Stat getStat(final String path) {
    long startT = System.nanoTime();

    try {
      Stat stat = retryUntilConnected(new Callable<Stat>() {

        @Override
        public Stat call() throws Exception {
          Stat stat = ((ZkConnection) _connection).getZookeeper().exists(path, false);
          return stat;
        }
      });
      increaseReadCounters(path);
      return stat;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  // override exists(path, watch), so we can record all exists requests
  @Override
  protected boolean exists(final String path, final boolean watch) {
    long startT = System.nanoTime();

    try {
      boolean exists = retryUntilConnected(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return _connection.exists(path, watch);
        }
      });
      increaseReadCounters(path);
      return exists;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  // override getChildren(path, watch), so we can record all getChildren requests
  @Override
  protected List<String> getChildren(final String path, final boolean watch) {
    long startT = System.nanoTime();

    try {
      List<String> children = retryUntilConnected(new Callable<List<String>>() {
        @Override
        public List<String> call() throws Exception {
          return _connection.getChildren(path, watch);
        }
      });
      increaseReadCounters(path);
      return children;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getChildren, path: " + path + ", time: " + (endT - startT) + " ns");
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
    long startT = System.nanoTime();
    try {
      byte[] data = retryUntilConnected(new Callable<byte[]>() {

        @Override
        public byte[] call() throws Exception {
          return _connection.readData(path, stat, watch);
        }
      });
      increaseReadCounters(path, data);
      return (T) deserialize(data, path);
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getData, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T extends Object> T readDataAndStat(String path, Stat stat,
      boolean returnNullIfPathNotExists) {
    T data = null;
    try {
      data = (T) super.readData(path, stat);
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
    long startT = System.nanoTime();
    try {
      final byte[] data = serialize(datat, path);
      checkDataSizeLimit(data);
      retryUntilConnected(new Callable<Object>() {

        @Override
        public Object call() throws Exception {
          _connection.writeData(path, data, expectedVersion);
          return null;
        }
      });
      increaseWriteCounters(path, data);
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  public Stat writeDataGetStat(final String path, Object datat, final int expectedVersion)
      throws InterruptedException {
    long start = System.nanoTime();
    try {
      final byte[] data = _zkSerializer.serialize(datat, path);
      checkDataSizeLimit(data);
      Stat stat =  retryUntilConnected(new Callable<Stat>() {

        @Override
        public Stat call() throws Exception {
          return ((ZkConnection) _connection).getZookeeper().setData(path, data, expectedVersion);
        }
      });
      increaseWriteCounters(path, data);
      return stat;
    } finally {
      long end = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData, path: " + path + ", time: " + (end - start) + " ns");
      }
    }
  }

  @Override
  public String create(final String path, Object datat, final CreateMode mode)
      throws IllegalArgumentException, ZkException {
    if (path == null) {
      throw new NullPointerException("path must not be null.");
    }

    long startT = System.nanoTime();
    try {
      final byte[] data = datat == null ? null : serialize(datat, path);
      checkDataSizeLimit(data);

      String actualPath = retryUntilConnected(new Callable<String>() {
        @Override
        public String call() throws Exception {
          return _connection.create(path, data, mode);
        }
      });
      increaseWriteCounters(path, data);
      return actualPath;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("create, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  @Override
  public boolean delete(final String path) {
    long startT = System.nanoTime();
    try {
      try {
        retryUntilConnected(new Callable<Object>() {

          @Override
          public Object call() throws Exception {
            _connection.delete(path);
            return null;
          }
        });
        return true;
      } catch (ZkNoNodeException e) {
        return false;
      }
    } finally {
      increaseWriteCounters(path);
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("delete, path: " + path + ", time: " + (endT - startT) + " ns");
      }
    }
  }

  public void asyncCreate(final String path, Object datat, final CreateMode mode,
      final CreateCallbackHandler cb) {
    final byte[] data = (datat == null ? null : serialize(datat, path));
    retryUntilConnected(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().create(path, data, Ids.OPEN_ACL_UNSAFE, // Arrays.asList(DEFAULT_ACL),
            mode, cb, null);
        return null;
      }
    });
  }

  public void asyncSetData(final String path, Object datat, final int version,
      final SetDataCallbackHandler cb) {
    final byte[] data = serialize(datat, path);
    retryUntilConnected(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().setData(path, data, version, cb, null);
        return null;
      }
    });
  }

  public void asyncGetData(final String path, final GetDataCallbackHandler cb) {
    retryUntilConnected(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().getData(path, null, cb, null);
        return null;
      }
    });
  }

  public void asyncExists(final String path, final ExistsCallbackHandler cb) {
    retryUntilConnected(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().exists(path, null, cb, null);
        return null;
      }
    });
  }

  public void asyncDelete(final String path, final DeleteCallbackHandler cb) {
    retryUntilConnected(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        ((ZkConnection) _connection).getZookeeper().delete(path, -1, cb, null);
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

  private void increaseReadCounters(String path) {
    increaseReadCounters(path, null);
  }

  private void increaseReadCounters(String path, byte[] data) {
    if (_monitor != null) {
      if (data == null) {
        _monitor.increaseReadCounters(path);
      } else {
        _monitor.increaseReadCounters(path, data.length);
      }
    }
  }

  private void increaseWriteCounters(String path) {
    increaseWriteCounters(path, null);
  }

  private void increaseWriteCounters(String path, byte[] data) {
    if (_monitor != null) {
      if (data == null) {
        _monitor.increaseWriteCounters(path);
      } else {
        _monitor.increaseWriteCounters(path, data.length);
      }
    }
  }
}
