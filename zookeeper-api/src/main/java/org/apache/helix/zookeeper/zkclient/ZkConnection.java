package org.apache.helix.zookeeper.zkclient;

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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkConnection implements IZkConnection {
  private static final Logger LOG = LoggerFactory.getLogger(ZkConnection.class);

  /** It is recommended to use quite large sessions timeouts for ZooKeeper. */
  private static final int DEFAULT_SESSION_TIMEOUT = 30000;

  // A config to force disabling using ZK's paginated getChildren.
  // By default the value is false.
  private static final boolean GETCHILDREN_PAGINATION_DISABLED =
      Boolean.getBoolean(ZkSystemPropertyKeys.ZK_GETCHILDREN_PAGINATION_DISABLED);

  @VisibleForTesting
  protected ZooKeeper _zk = null;
  private Lock _zookeeperLock = new ReentrantLock();
  private Method _getChildrenMethod;

  private final String _servers;
  private final int _sessionTimeOut;

  public ZkConnection(String zkServers) {
    this(zkServers, DEFAULT_SESSION_TIMEOUT);
  }

  public ZkConnection(String zkServers, int sessionTimeOut) {
    _servers = zkServers;
    _sessionTimeOut = sessionTimeOut;
  }

  @Override
  public void connect(Watcher watcher) {
    _zookeeperLock.lock();
    try {
      if (_zk != null) {
        throw new IllegalStateException("zk client has already been started");
      }
      try {
        LOG.debug("Creating new ZookKeeper instance to connect to " + _servers + ".");
        _zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
      } catch (IOException e) {
        throw new ZkException("Unable to connect to " + _servers, e);
      }
    } finally {
      _zookeeperLock.unlock();
    }
  }

  @Override
  public void close() throws InterruptedException {
    _zookeeperLock.lock();
    try {
      if (_zk != null) {
        LOG.debug("Closing ZooKeeper connected to " + _servers);
        _zk.close();
        _zk = null;
      }
    } finally {
      _zookeeperLock.unlock();
    }
  }

  protected void reconnect(Watcher watcher) throws InterruptedException {
    _zookeeperLock.lock();
    try {
      if (_zk == null) {
        throw new IllegalStateException("zk client has not been connected or already been closed");
      }
      ZooKeeper prevZk = _zk;
      try {
        LOG.debug("Creating new ZookKeeper instance to reconnect to " + _servers + ".");
        _zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
        prevZk.close();
      } catch (IOException e) {
        throw new ZkException("Unable to connect to " + _servers, e);
      }
    } finally {
      _zookeeperLock.unlock();
    }
  }

  @Override
  public String create(String path, byte[] data, CreateMode mode)
      throws KeeperException, InterruptedException {
    return _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
  }

  @Override
  public String create(String path, byte[] data, List<ACL> acl, CreateMode mode)
      throws KeeperException, InterruptedException {
    return _zk.create(path, data, acl, mode);
  }

  @Override
  public String create(String path, byte[] data, List<ACL> acl, CreateMode mode, long ttl)
      throws KeeperException, InterruptedException {
    return _zk.create(path, data, acl, mode, null, ttl);
  }

  @Override
  public void delete(String path) throws InterruptedException, KeeperException {
    _zk.delete(path, -1);
  }

  @Override
  public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
    return _zk.exists(path, watch) != null;
  }

  /**
   * Returns a list of children of the given path.
   * <p>
   * If the watch is non-null and the call is successful (no exception is thrown),
   * a watch will be left on the node with the given path.
   * <p>
   * The implementation uses java reflection to check whether the native zk supports
   * paginated getChildren API:
   * <p>- if yes, and {@link #GETCHILDREN_PAGINATION_DISABLED} is false, call the paginated API;
   * <p>- otherwise, fall back to the non-paginated API.
   *
   * @param path the path of the node
   * @param watch a boolean flag to indicate whether the watch should be added to the node
   * @return a list of children of the given path
   * @throws KeeperException if the server signals an error with a non-zero error code
   * @throws InterruptedException if the server transaction is interrupted
   */
  @Override
  public List<String> getChildren(final String path, final boolean watch)
      throws KeeperException, InterruptedException {
    if (_getChildrenMethod == null) {
      lookupGetChildrenMethod();
    }

    try {
      // This cast is safe because the type passed in is also List<String>
      @SuppressWarnings("unchecked")
      List<String> children = (List<String>) _getChildrenMethod.invoke(_zk, path, watch);
      return children;
    } catch (InvocationTargetException e) {
      // Handle any exceptions thrown by method to be invoked
      handleInvokedMethodException(e.getCause());
    } catch (IllegalAccessException e) {
      // Log the exception to understand the detailed reason.
      LOG.error("Unable to get children for {}", path, e);
    }
    // If it reaches here, something must be wrong with the API.
    throw KeeperException.create(KeeperException.Code.APIERROR, path);
  }

  @Override
  public byte[] readData(String path, Stat stat, boolean watch)
      throws KeeperException, InterruptedException {
    return _zk.getData(path, watch, stat);
  }

  public void writeData(String path, byte[] data) throws KeeperException, InterruptedException {
    writeData(path, data, -1);
  }

  @Override
  public void writeData(String path, byte[] data, int version)
      throws KeeperException, InterruptedException {
    _zk.setData(path, data, version);
  }

  @Override
  public Stat writeDataReturnStat(String path, byte[] data, int version)
      throws KeeperException, InterruptedException {
    return _zk.setData(path, data, version);
  }

  @Override
  public States getZookeeperState() {
    return _zk != null ? _zk.getState() : null;
  }

  public ZooKeeper getZookeeper() {
    return _zk;
  }

  @Override
  public long getCreateTime(String path) throws KeeperException, InterruptedException {
    Stat stat = _zk.exists(path, false);
    if (stat != null) {
      return stat.getCtime();
    }
    return -1;
  }

  @Override
  public String getServers() {
    return _servers;
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException {
    return _zk.multi(ops);
  }

  @Override
  public void addAuthInfo(String scheme, byte[] auth) {
    _zk.addAuthInfo(scheme, auth);
  }

  private void lookupGetChildrenMethod() {
    _getChildrenMethod = doLookUpGetChildrenMethod();

    LOG.info("Pagination config {}={}, method to be invoked: {}",
        ZkSystemPropertyKeys.ZK_GETCHILDREN_PAGINATION_DISABLED, GETCHILDREN_PAGINATION_DISABLED,
        _getChildrenMethod.getName());
  }

  private Method doLookUpGetChildrenMethod() {
    if (!GETCHILDREN_PAGINATION_DISABLED) {
      try {
        // Lookup the paginated getChildren API
        return ZooKeeper.class.getMethod("getAllChildrenPaginated", String.class, boolean.class);
      } catch (NoSuchMethodException e) {
        LOG.info("Paginated getChildren is not supported, fall back to non-paginated getChildren");
      }
    }

    return lookupNonPaginatedGetChildren();
  }

  private Method lookupNonPaginatedGetChildren() {
    try {
      return ZooKeeper.class.getMethod("getChildren", String.class, boolean.class);
    } catch (NoSuchMethodException e) {
      // We should not expect this exception here.
      throw ExceptionUtil.convertToRuntimeException(e.getCause());
    }
  }

  private void handleInvokedMethodException(Throwable cause)
      throws KeeperException, InterruptedException {
    if (cause instanceof KeeperException.UnimplementedException) {
      LOG.warn("Paginated getChildren is unimplemented in ZK server! "
          + "Falling back to non-paginated getChildren");
      _getChildrenMethod = lookupNonPaginatedGetChildren();
      // ZK server would disconnect this connection because of UnimplementedException.
      // Throw CONNECTIONLOSS so ZkClient can retry.
      // TODO: handle it in a better way without throwing CONNECTIONLOSS
      throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
    } else if (cause instanceof KeeperException) {
      throw KeeperException.create(((KeeperException) cause).code());
    } else if (cause instanceof InterruptedException) {
      throw new InterruptedException(cause.getMessage());
    } else {
      throw ExceptionUtil.convertToRuntimeException(cause);
    }
  }
}
