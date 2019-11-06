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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.CreateCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.DeleteCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.ExistsCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.GetDataCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.SetDataCallbackHandler;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.zookeeper.ZkConnection;
import org.apache.helix.store.zk.ZNode;
import org.apache.helix.util.HelixUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBaseDataAccessor<T> implements BaseDataAccessor<T> {
  enum RetCode {
    OK,
    NODE_EXISTS,
    ERROR
  }

  /**
   * struct holding return information
   */
  public class AccessResult {
    RetCode _retCode;
    List<String> _pathCreated;

    Stat _stat;

    /**
     * used by update only
     */
    T _updatedValue;

    public AccessResult() {
      _retCode = RetCode.ERROR;
      _pathCreated = new ArrayList<>();
      _stat = new Stat();
      _updatedValue = null;
    }
  }

  private static Logger LOG = LoggerFactory.getLogger(ZkBaseDataAccessor.class);

  private final HelixZkClient _zkClient;
  // onDemand ZnRecordClient
  private HelixZkClient _nonZNRecordClient = null;

  public ZkBaseDataAccessor(HelixZkClient zkClient) {
    if (zkClient == null) {
      throw new NullPointerException("zkclient is null");
    }
    _zkClient = zkClient;
  }

  private synchronized HelixZkClient getNonZNRecordClient() {
    if (_nonZNRecordClient == null) {
      _nonZNRecordClient = new ZkClient.Builder().setConnection(
          new ZkConnection(_zkClient.getServers(), ZkClient.DEFAULT_SESSION_TIMEOUT))
          .setZkSerializer(new ZkSerializer() {
            @Override
            public byte[] serialize(Object data) throws ZkMarshallingError {
              if (data instanceof byte[]) {
                return (byte[]) data;
              }
              throw new HelixException("Only support a byte array as an argument!");
            }

            @Override
            public Object deserialize(byte[] data) throws ZkMarshallingError {
              return data;
            }
          }).build();
    }

    return _nonZNRecordClient;
  }

  /**
   * sync create
   */
  @Override
  public boolean create(String path, T record, int options) {
    AccessResult result = doCreate(path, record, options, true);
    return result._retCode == RetCode.OK;
  }

  /**
   * sync create. In order to support non-ZnRecord creation, the instance of {@link HelixZkClient} needs to be a parameter
   */
  public AccessResult doCreate(String path, Object record, int options, boolean isZnRecord) {
    AccessResult result = new AccessResult();
    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      LOG.error("Invalid create mode. options: " + options);
      result._retCode = RetCode.ERROR;
      return result;
    }

    HelixZkClient zkClient = isZnRecord ? _zkClient : getNonZNRecordClient();
    boolean retry;
    do {
      retry = false;
      try {
        zkClient.create(path, record, mode);
        result._pathCreated.add(path);
        result._retCode = RetCode.OK;
        return result;
      } catch (ZkNoNodeException e) {
        // this will happen if parent node does not exist
        String parentPath = HelixUtil.getZkParentPath(path);
        try {
          AccessResult res = doCreate(parentPath, null, AccessOption.PERSISTENT, isZnRecord);
          result._pathCreated.addAll(res._pathCreated);
          RetCode rc = res._retCode;
          if (rc == RetCode.OK || rc == RetCode.NODE_EXISTS) {
            // if parent node created/exists, retry
            retry = true;
          }
        } catch (Exception e1) {
          LOG.error("Exception while creating path: " + parentPath, e1);
          result._retCode = RetCode.ERROR;
          return result;
        }
      } catch (ZkNodeExistsException e) {
        LOG.warn("Node already exists. path: " + path);
        result._retCode = RetCode.NODE_EXISTS;
        return result;
      } catch (Exception e) {
        LOG.error("Exception while creating path: " + path, e);
        result._retCode = RetCode.ERROR;
        return result;
      }
    } while (retry);

    result._retCode = RetCode.OK;
    return result;
  }

  /**
   * sync set
   */
  @Override
  public boolean set(String path, T record, int options) {
    return set(path, record, -1, options);
  }

  /**
   * sync set
   */
  @Override
  public boolean set(String path, T record, int expectVersion, int options) {
    AccessResult result = doSet(path, record, expectVersion, options);
    return result._retCode == RetCode.OK;
  }

  /**
   * sync set
   */
  public AccessResult doSet(String path, T record, int expectVersion, int options) {
    AccessResult result = new AccessResult();

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      LOG.error("Invalid set mode. options: " + options);
      result._retCode = RetCode.ERROR;
      return result;
    }

    boolean retry;
    do {
      retry = false;
      try {
        Stat stat = _zkClient.writeDataGetStat(path, record, expectVersion);
        DataTree.copyStat(stat, result._stat);
      } catch (ZkNoNodeException e) {
        // node not exists, try create if expectedVersion == -1; in this case, stat will not be set
        if (expectVersion != -1) {
          LOG.error("Could not create node if expectVersion != -1, was " + expectVersion);
          result._retCode = RetCode.ERROR;
          return result;
        }
        try {
          // may create recursively
          AccessResult res = doCreate(path, record, options, true);
          result._pathCreated.addAll(res._pathCreated);
          RetCode rc = res._retCode;
          switch (rc) {
          case OK:
            // not set stat if node is created (instead of set)
            break;
          case NODE_EXISTS:
            retry = true;
            break;
          default:
            LOG.error("Fail to set path by creating: " + path);
            result._retCode = RetCode.ERROR;
            return result;
          }
        } catch (Exception e1) {
          LOG.error("Exception while setting path by creating: " + path, e);
          result._retCode = RetCode.ERROR;
          return result;
        }
      } catch (ZkBadVersionException e) {
        LOG.debug("Exception while setting path: " + path, e);
        throw e;
      } catch (Exception e) {
        LOG.error("Exception while setting path: " + path, e);
        result._retCode = RetCode.ERROR;
        return result;
      }
    } while (retry);

    result._retCode = RetCode.OK;
    return result;
  }

  /**
   * sync update
   */
  @Override
  public boolean update(String path, DataUpdater<T> updater, int options) {
    AccessResult result = doUpdate(path, updater, options);
    return result._retCode == RetCode.OK;
  }

  /**
   * sync update
   */
  public AccessResult doUpdate(String path, DataUpdater<T> updater, int options) {
    AccessResult result = new AccessResult();
    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      LOG.error("Invalid update mode. options: " + options);
      result._retCode = RetCode.ERROR;
      return result;
    }

    boolean retry;
    T updatedData = null;
    do {
      retry = false;
      try {
        Stat readStat = new Stat();
        T oldData = (T) _zkClient.readData(path, readStat);
        T newData = updater.update(oldData);
        if (newData != null) {
          Stat setStat = _zkClient.writeDataGetStat(path, newData, readStat.getVersion());
          DataTree.copyStat(setStat, result._stat);
        }
        updatedData = newData;
      } catch (ZkBadVersionException e) {
        retry = true;
      } catch (ZkNoNodeException e) {
        // node not exist, try create, pass null to updater
        try {
          T newData = updater.update(null);
          RetCode rc;
          if (newData != null) {
            AccessResult res = doCreate(path, newData, options, true);
            result._pathCreated.addAll(res._pathCreated);
            rc = res._retCode;
          } else {
            // If update returns null, no need to create.
            rc = RetCode.OK;
          }
          switch (rc) {
          case OK:
            updatedData = newData;
            break;
          case NODE_EXISTS:
            retry = true;
            break;
          default:
            LOG.error("Fail to update path by creating: " + path);
            result._retCode = RetCode.ERROR;
            return result;
          }
        } catch (Exception e1) {
          LOG.error("Exception while updating path by creating: " + path, e1);
          result._retCode = RetCode.ERROR;
          return result;
        }
      } catch (Exception e) {
        LOG.error("Exception while updating path: " + path, e);
        result._retCode = RetCode.ERROR;
        return result;
      }
    } while (retry);

    result._retCode = RetCode.OK;
    result._updatedValue = updatedData;
    return result;
  }

  /**
   * sync get
   */
  @Override
  public T get(String path, Stat stat, int options) {
    T data = null;
    try {
      data = (T) _zkClient.readData(path, stat);
    } catch (ZkNoNodeException e) {
      if (AccessOption.isThrowExceptionIfNotExist(options)) {
        throw e;
      }
    }
    return data;
  }

  /**
   * Sync get method with custom serializer support
   */
  public Object get(String path, Stat stat, int options, ZkSerializer serializer) {
    byte[] content = null;
    try {
      content = getNonZNRecordClient().readData(path, stat);
      return serializer.deserialize(content);
    } catch (ZkNoNodeException ex) {
      if (AccessOption.isThrowExceptionIfNotExist(options)) {
        throw ex;
      }
    } catch (ZkMarshallingError ex) {
      LOG.error("Failed to deserialize the byte array", ex);
      throw ex;
    }
    return content;
  }

  /**
   * Sync create method with custom serializer support
   */
  public boolean create(String path, Object data, int options, ZkSerializer serializer) {
    byte[] bytes = serializer.serialize(data);
    AccessResult result = doCreate(path, bytes, options, false);
    return result._retCode == RetCode.OK;
  }

  /**
   * async get
   */
  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options) {
    boolean[] needRead = new boolean[paths.size()];
    Arrays.fill(needRead, true);

    return get(paths, stats, needRead, false);
  }

  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options, boolean throwException)
      throws HelixException {
    boolean[] needRead = new boolean[paths.size()];
    Arrays.fill(needRead, true);

    return get(paths, stats, needRead, throwException);
  }

  /**
   * async get
   */
  List<T> get(List<String> paths, List<Stat> stats, boolean[] needRead, boolean throwException)
      throws HelixException {
    if (paths == null || paths.size() == 0) {
      return Collections.emptyList();
    }

    // init stats
    if (stats != null) {
      stats.clear();
      stats.addAll(Collections.<Stat> nCopies(paths.size(), null));
    }

    long startT = System.nanoTime();

    try {
      // issue asyn get requests
      GetDataCallbackHandler[] cbList = new GetDataCallbackHandler[paths.size()];
      for (int i = 0; i < paths.size(); i++) {
        if (!needRead[i])
          continue;

        String path = paths.get(i);
        cbList[i] = new GetDataCallbackHandler();
        _zkClient.asyncGetData(path, cbList[i]);
      }

      // wait for completion
      for (int i = 0; i < cbList.length; i++) {
        if (!needRead[i])
          continue;

        GetDataCallbackHandler cb = cbList[i];
        cb.waitForSuccess();
      }

      // construct return results
      List<T> records = new ArrayList<T>(Collections.<T> nCopies(paths.size(), null));
      Map<String, Integer> pathFailToRead = new HashMap<>();
      for (int i = 0; i < paths.size(); i++) {
        if (!needRead[i])
          continue;

        GetDataCallbackHandler cb = cbList[i];
        if (Code.get(cb.getRc()) == Code.OK) {
          @SuppressWarnings("unchecked")
          T record = (T) _zkClient.deserialize(cb._data, paths.get(i));
          records.set(i, record);
          if (stats != null) {
            stats.set(i, cb._stat);
          }
        } else if (Code.get(cb.getRc()) != Code.NONODE && throwException) {
          throw new HelixMetaDataAccessException(
              String.format("Failed to read node %s", paths.get(i)));
        } else {
          pathFailToRead.put(paths.get(i), cb.getRc());
        }
      }
      if (pathFailToRead.size() > 0) {
        LOG.warn("Fail to read record for paths: " + pathFailToRead);
      }
      return records;
    } catch (Exception e) {
      throw new HelixMetaDataAccessException(String.format("Fail to read nodes for %s", paths));
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getData_async, size: " + paths.size() + ", paths: " + paths.get(0)
            + ",... time: " + (endT - startT) + " ns");
      }
    }
  }

  /**
   * asyn getChildren
   * The retryCount and retryInterval will be ignored.
   */
  // TODO: Change the behavior of getChildren when Helix starts migrating API.
  @Override
  public List<T> getChildren(String parentPath, List<Stat> stats, int options) {
    return getChildren(parentPath, stats, options, false);
  }

  @Override
  public List<T> getChildren(String parentPath, List<Stat> stats, int options, int retryCount,
      int retryInterval) throws HelixException {
    int readCount = retryCount + 1;
    while (readCount > 0) {
      try {
        readCount--;
        List<T> records = getChildren(parentPath, stats, options, true);
        return records;
      } catch (HelixMetaDataAccessException e) {
        if (readCount == 0) {
          throw new HelixMetaDataAccessException(
              String.format("Failed to get full list of %s", parentPath), e);
        }
        try {
          Thread.sleep(retryInterval);
        } catch (InterruptedException interruptedException) {
          throw new HelixMetaDataAccessException("Fail to interrupt the sleep",
              interruptedException);
        }
      }
    }

    // Impossible to reach end
    return null;
  }

  private List<T> getChildren(String parentPath, List<Stat> stats, int options,
      boolean throwException) {
    try {
      // prepare child paths
      List<String> childNames = getChildNames(parentPath, options);
      if (childNames == null || childNames.size() == 0) {
        return Collections.emptyList();
      }

      List<String> paths = new ArrayList<>();
      for (String childName : childNames) {
        String path = parentPath + "/" + childName;
        paths.add(path);
      }

      // remove null record
      List<Stat> curStats = new ArrayList<>(paths.size());
      boolean[] needRead = new boolean[paths.size()];
      Arrays.fill(needRead, true);
      List<T> records = get(paths, curStats, needRead, throwException);
      Iterator<T> recordIter = records.iterator();
      Iterator<Stat> statIter = curStats.iterator();
      while (statIter.hasNext()) {
        recordIter.next();
        if (statIter.next() == null) {
          statIter.remove();
          recordIter.remove();
        }
      }

      if (stats != null) {
        stats.clear();
        stats.addAll(curStats);
      }

      return records;
    } catch (ZkNoNodeException e) {
      return Collections.emptyList();
    }
  }

  /**
   * sync getChildNames
   * @return null if parentPath doesn't exist
   */
  @Override
  public List<String> getChildNames(String parentPath, int options) {
    try {
      List<String> childNames = _zkClient.getChildren(parentPath);
      Collections.sort(childNames);
      return childNames;
    } catch (ZkNoNodeException e) {
      return null;
    }
  }

  /**
   * sync exists
   */
  @Override
  public boolean exists(String path, int options) {
    return _zkClient.exists(path);
  }

  /**
   * sync getStat
   */
  @Override
  public Stat getStat(String path, int options) {
    return _zkClient.getStat(path);
  }

  /**
   * Sync remove. it tries to remove the ZNode and all its descendants if any, node does not exist
   * is regarded as success
   */
  @Override
  public boolean remove(String path, int options) {
    try {
      // operation will not throw exception when path successfully deleted or does not exist
      // despite real error, operation will throw exception when path not empty, and in this
      // case, we try to delete recursively
      _zkClient.delete(path);
    } catch (ZkException e) {
      LOG.debug("Failed to delete {} with opts {}, err: {}. Try recursive delete", path, options,
          e.getMessage());
      try {
        _zkClient.deleteRecursively(path);
      } catch (HelixException he) {
        LOG.error("Failed to delete {} recursively with opts {}.", path, options, he);
        return false;
      }
    }
    return true;
  }

  /**
   * async create. give up on error other than NONODE
   */
  CreateCallbackHandler[] create(List<String> paths, List<T> records, boolean[] needCreate,
      List<List<String>> pathsCreated, int options) {
    if ((records != null && records.size() != paths.size()) || needCreate.length != paths.size()
        || (pathsCreated != null && pathsCreated.size() != paths.size())) {
      throw new IllegalArgumentException(
          "paths, records, needCreate, and pathsCreated should be of same size");
    }

    CreateCallbackHandler[] cbList = new CreateCallbackHandler[paths.size()];

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      LOG.error("Invalid async set mode. options: " + options);
      return cbList;
    }

    boolean retry;
    do {
      retry = false;

      for (int i = 0; i < paths.size(); i++) {
        if (!needCreate[i])
          continue;

        String path = paths.get(i);
        T record = records == null ? null : records.get(i);
        cbList[i] = new CreateCallbackHandler();
        _zkClient.asyncCreate(path, record, mode, cbList[i]);
      }

      List<String> parentPaths = new ArrayList<>(Collections.<String> nCopies(paths.size(), null));
      boolean failOnNoNode = false;

      for (int i = 0; i < paths.size(); i++) {
        if (!needCreate[i])
          continue;

        CreateCallbackHandler cb = cbList[i];
        cb.waitForSuccess();
        String path = paths.get(i);

        if (Code.get(cb.getRc()) == Code.NONODE) {
          String parentPath = HelixUtil.getZkParentPath(path);
          parentPaths.set(i, parentPath);
          failOnNoNode = true;
        } else {
          // if create succeed or fail on error other than NONODE,
          // give up
          needCreate[i] = false;

          // if succeeds, record what paths we've created
          if (Code.get(cb.getRc()) == Code.OK && pathsCreated != null) {
            if (pathsCreated.get(i) == null) {
              pathsCreated.set(i, new ArrayList<String>());
            }
            pathsCreated.get(i).add(path);
          }
        }
      }

      if (failOnNoNode) {
        boolean[] needCreateParent = Arrays.copyOf(needCreate, needCreate.length);

        CreateCallbackHandler[] parentCbList =
            create(parentPaths, null, needCreateParent, pathsCreated, AccessOption.PERSISTENT);
        for (int i = 0; i < parentCbList.length; i++) {
          CreateCallbackHandler parentCb = parentCbList[i];
          if (parentCb == null)
            continue;

          Code rc = Code.get(parentCb.getRc());

          // if parent is created, retry create child
          if (rc == Code.OK || rc == Code.NODEEXISTS) {
            retry = true;
            break;
          }
        }
      }
    } while (retry);

    return cbList;
  }

  /**
   * async create
   * TODO: rename to create
   */
  @Override
  public boolean[] createChildren(List<String> paths, List<T> records, int options) {
    boolean[] success = new boolean[paths.size()];

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      LOG.error("Invalid async create mode. options: " + options);
      return success;
    }

    boolean[] needCreate = new boolean[paths.size()];
    Arrays.fill(needCreate, true);
    List<List<String>> pathsCreated =
        new ArrayList<>(Collections.<List<String>> nCopies(paths.size(), null));

    long startT = System.nanoTime();
    try {

      CreateCallbackHandler[] cbList = create(paths, records, needCreate, pathsCreated, options);

      for (int i = 0; i < cbList.length; i++) {
        CreateCallbackHandler cb = cbList[i];
        success[i] = (Code.get(cb.getRc()) == Code.OK);
      }

      return success;

    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("create_async, size: " + paths.size() + ", paths: " + paths.get(0) + ",... time: "
            + (endT - startT) + " ns");
      }
    }
  }

  /**
   * async set
   * TODO: rename to set
   */
  @Override
  public boolean[] setChildren(List<String> paths, List<T> records, int options) {
    return set(paths, records, null, null, options);
  }

  /**
   * async set, give up on error other than NoNode
   */
  boolean[] set(List<String> paths, List<T> records, List<List<String>> pathsCreated,
      List<Stat> stats, int options) {
    if (paths == null || paths.size() == 0) {
      return new boolean[0];
    }

    if ((records != null && records.size() != paths.size())
        || (pathsCreated != null && pathsCreated.size() != paths.size())) {
      throw new IllegalArgumentException("paths, records, and pathsCreated should be of same size");
    }

    boolean[] success = new boolean[paths.size()];

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      LOG.error("Invalid async set mode. options: " + options);
      return success;
    }

    List<Stat> setStats = new ArrayList<>(Collections.<Stat> nCopies(paths.size(), null));
    SetDataCallbackHandler[] cbList = new SetDataCallbackHandler[paths.size()];
    CreateCallbackHandler[] createCbList = null;
    boolean[] needSet = new boolean[paths.size()];
    Arrays.fill(needSet, true);

    long startT = System.nanoTime();

    try {
      boolean retry;
      do {
        retry = false;

        for (int i = 0; i < paths.size(); i++) {
          if (!needSet[i])
            continue;

          String path = paths.get(i);
          T record = records.get(i);
          cbList[i] = new SetDataCallbackHandler();
          _zkClient.asyncSetData(path, record, -1, cbList[i]);

        }

        boolean failOnNoNode = false;

        for (int i = 0; i < cbList.length; i++) {
          SetDataCallbackHandler cb = cbList[i];
          cb.waitForSuccess();
          Code rc = Code.get(cb.getRc());
          switch (rc) {
          case OK:
            setStats.set(i, cb.getStat());
            needSet[i] = false;
            break;
          case NONODE:
            // if fail on NoNode, try create the node
            failOnNoNode = true;
            break;
          default:
            // if fail on error other than NoNode, give up
            needSet[i] = false;
            break;
          }
        }

        // if failOnNoNode, try create
        if (failOnNoNode) {
          boolean[] needCreate = Arrays.copyOf(needSet, needSet.length);
          createCbList = create(paths, records, needCreate, pathsCreated, options);
          for (int i = 0; i < createCbList.length; i++) {
            CreateCallbackHandler createCb = createCbList[i];
            if (createCb == null) {
              continue;
            }

            Code rc = Code.get(createCb.getRc());
            switch (rc) {
            case OK:
              setStats.set(i, ZNode.ZERO_STAT);
              needSet[i] = false;
              break;
            case NODEEXISTS:
              retry = true;
              break;
            default:
              // if creation fails on error other than NodeExists
              // no need to retry set
              needSet[i] = false;
              break;
            }
          }
        }
      } while (retry);

      // construct return results
      for (int i = 0; i < cbList.length; i++) {
        SetDataCallbackHandler cb = cbList[i];

        Code rc = Code.get(cb.getRc());
        if (rc == Code.OK) {
          success[i] = true;
        } else if (rc == Code.NONODE) {
          CreateCallbackHandler createCb = createCbList[i];
          if (Code.get(createCb.getRc()) == Code.OK) {
            success[i] = true;
          }
        }
      }

      if (stats != null) {
        stats.clear();
        stats.addAll(setStats);
      }

      return success;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData_async, size: " + paths.size() + ", paths: " + paths.get(0)
            + ",... time: " + (endT - startT) + " ns");
      }
    }
  }

  // TODO: rename to update
  /**
   * async update
   */
  @Override
  public boolean[] updateChildren(List<String> paths, List<DataUpdater<T>> updaters, int options) {

    List<T> updateData = update(paths, updaters, null, null, options);
    boolean[] success = new boolean[paths.size()]; // init to false
    for (int i = 0; i < paths.size(); i++) {
      T data = updateData.get(i);
      success[i] = (data != null);
    }
    return success;
  }

  /**
   * async update
   * return: updatedData on success or null on fail
   */
  List<T> update(List<String> paths, List<DataUpdater<T>> updaters, List<List<String>> pathsCreated,
      List<Stat> stats, int options) {
    if (paths == null || paths.size() == 0) {
      LOG.error("paths is null or empty");
      return Collections.emptyList();
    }

    if (updaters.size() != paths.size()
        || (pathsCreated != null && pathsCreated.size() != paths.size())) {
      throw new IllegalArgumentException(
          "paths, updaters, and pathsCreated should be of same size");
    }

    List<Stat> setStats = new ArrayList<Stat>(Collections.<Stat> nCopies(paths.size(), null));
    List<T> updateData = new ArrayList<T>(Collections.<T> nCopies(paths.size(), null));

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      LOG.error("Invalid update mode. options: " + options);
      return updateData;
    }

    SetDataCallbackHandler[] cbList = new SetDataCallbackHandler[paths.size()];
    CreateCallbackHandler[] createCbList = null;
    boolean[] needUpdate = new boolean[paths.size()];
    Arrays.fill(needUpdate, true);

    long startT = System.nanoTime();

    try {
      boolean retry;
      do {
        retry = false;
        boolean[] needCreate = new boolean[paths.size()]; // init'ed with false
        boolean failOnNoNode = false;

        // asycn read all data
        List<Stat> curStats = new ArrayList<Stat>();
        List<T> curDataList =
            get(paths, curStats, Arrays.copyOf(needUpdate, needUpdate.length), false);

        // async update
        List<T> newDataList = new ArrayList<T>();
        for (int i = 0; i < paths.size(); i++) {
          if (!needUpdate[i]) {
            newDataList.add(null);
            continue;
          }
          String path = paths.get(i);
          DataUpdater<T> updater = updaters.get(i);
          T newData = updater.update(curDataList.get(i));
          newDataList.add(newData);
          if (newData == null) {
            // No need to create or update if the updater does not return a new version
            continue;
          }
          Stat curStat = curStats.get(i);
          if (curStat == null) {
            // node not exists
            failOnNoNode = true;
            needCreate[i] = true;
          } else {
            cbList[i] = new SetDataCallbackHandler();
            _zkClient.asyncSetData(path, newData, curStat.getVersion(), cbList[i]);
          }
        }

        // wait for completion
        boolean failOnBadVersion = false;

        for (int i = 0; i < paths.size(); i++) {
          SetDataCallbackHandler cb = cbList[i];
          if (cb == null)
            continue;

          cb.waitForSuccess();

          switch (Code.get(cb.getRc())) {
          case OK:
            updateData.set(i, newDataList.get(i));
            setStats.set(i, cb.getStat());
            needUpdate[i] = false;
            break;
          case NONODE:
            failOnNoNode = true;
            needCreate[i] = true;
            break;
          case BADVERSION:
            failOnBadVersion = true;
            break;
          default:
            // if fail on error other than NoNode or BadVersion
            // will not retry
            needUpdate[i] = false;
            break;
          }
        }

        // if failOnNoNode, try create
        if (failOnNoNode) {
          createCbList = create(paths, newDataList, needCreate, pathsCreated, options);
          for (int i = 0; i < paths.size(); i++) {
            CreateCallbackHandler createCb = createCbList[i];
            if (createCb == null) {
              continue;
            }

            switch (Code.get(createCb.getRc())) {
            case OK:
              needUpdate[i] = false;
              updateData.set(i, newDataList.get(i));
              setStats.set(i, ZNode.ZERO_STAT);
              break;
            case NODEEXISTS:
              retry = true;
              break;
            default:
              // if fail on error other than NodeExists
              // will not retry
              needUpdate[i] = false;
              break;
            }
          }
        }

        // if failOnBadVersion, retry
        if (failOnBadVersion) {
          retry = true;
        }
      } while (retry);

      if (stats != null) {
        stats.clear();
        stats.addAll(setStats);
      }

      return updateData;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData_async, size: " + paths.size() + ", paths: " + paths.get(0)
            + ",... time: " + (endT - startT) + " ns");
      }
    }

  }

  /**
   * async exists
   */
  @Override
  public boolean[] exists(List<String> paths, int options) {
    Stat[] stats = getStats(paths, options);

    boolean[] exists = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      exists[i] = (stats[i] != null);
    }

    return exists;
  }

  /**
   * async getStat
   */
  @Override
  public Stat[] getStats(List<String> paths, int options) {
    if (paths == null || paths.size() == 0) {
      LOG.error("paths is null or empty");
      return new Stat[0];
    }

    Stat[] stats = new Stat[paths.size()];

    long startT = System.nanoTime();

    try {
      ExistsCallbackHandler[] cbList = new ExistsCallbackHandler[paths.size()];
      for (int i = 0; i < paths.size(); i++) {
        String path = paths.get(i);
        cbList[i] = new ExistsCallbackHandler();
        _zkClient.asyncExists(path, cbList[i]);
      }

      for (int i = 0; i < cbList.length; i++) {
        ExistsCallbackHandler cb = cbList[i];
        cb.waitForSuccess();
        stats[i] = cb._stat;
      }

      return stats;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("exists_async, size: " + paths.size() + ", paths: " + paths.get(0) + ",... time: "
            + (endT - startT) + " ns");
      }
    }
  }

  /**
   * async remove
   */
  @Override
  public boolean[] remove(List<String> paths, int options) {
    if (paths == null || paths.size() == 0) {
      return new boolean[0];
    }

    boolean[] success = new boolean[paths.size()];

    DeleteCallbackHandler[] cbList = new DeleteCallbackHandler[paths.size()];

    long startT = System.nanoTime();

    try {
      for (int i = 0; i < paths.size(); i++) {
        String path = paths.get(i);
        cbList[i] = new DeleteCallbackHandler();
        _zkClient.asyncDelete(path, cbList[i]);
      }

      for (int i = 0; i < cbList.length; i++) {
        DeleteCallbackHandler cb = cbList[i];
        cb.waitForSuccess();
        success[i] = (cb.getRc() == 0);
      }

      return success;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("delete_async, size: " + paths.size() + ", paths: " + paths.get(0) + ",... time: "
            + (endT - startT) + " ns");
      }
    }
  }

  /**
   * Subscribe to zookeeper data changes
   */
  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    _zkClient.subscribeDataChanges(path, listener);
  }

  /**
   * Unsubscribe to zookeeper data changes
   */
  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {
    _zkClient.unsubscribeDataChanges(path, dataListener);
  }

  /**
   * Subscrie to zookeeper data changes
   */
  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    return _zkClient.subscribeChildChanges(path, listener);
  }

  /**
   * Unsubscrie to zookeeper data changes
   */
  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener childListener) {
    _zkClient.unsubscribeChildChanges(path, childListener);
  }

  /**
   * Reset
   */
  @Override
  public void reset() {
    // Nothing to do
  }
}
