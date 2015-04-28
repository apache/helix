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
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.CreateCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.DeleteCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.ExistsCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.GetDataCallbackHandler;
import org.apache.helix.manager.zk.ZkAsyncCallbacks.SetDataCallbackHandler;
import org.apache.helix.store.zk.ZNode;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class ZkBaseDataAccessor<T> implements BaseDataAccessor<T> {
  /**
   * return code for zk operations
   */
  enum RetCode {
    OK,
    NODE_EXISTS,
    NONODE,
    ERROR
  }

  /**
   * structure holding return information
   */
  public class AccessResult {
    final RetCode _retCode;
    final List<String> _pathCreated;

    final Stat _stat;

    final T _resultValue;

    public AccessResult(RetCode retCode) {
      this(retCode, null, null, null);
    }

    public AccessResult(RetCode retCode, List<String> pathCreated, Stat stat, T resultValue) {
      _retCode = retCode;
      _pathCreated = pathCreated;
      _stat = stat;
      _resultValue = resultValue;
    }
  }

  private static Logger LOG = Logger.getLogger(ZkBaseDataAccessor.class);

  private final ZkClient _zkClient;

  public ZkBaseDataAccessor(ZkClient zkClient) {
    if (zkClient == null) {
      throw new NullPointerException("zkclient is null");
    }
    _zkClient = zkClient;
  }

  /**
   * sync create a znode
   */
  @Override
  public boolean create(String path, T record, int options) {
    AccessResult result = doCreate(path, record, options);
    return result._retCode == RetCode.OK;
  }

  /**
   * sync create a znode. create parent znodes if necessary
   * @param path path to create
   * @param record value to create, null for no value
   * @param options
   * @return
   */
  public AccessResult doCreate(String path, T record, int options) {
    if (path == null) {
      throw new NullPointerException("path can't be null");
    }

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      throw new IllegalArgumentException("Invalid create options: " + options);
    }

    boolean retry;
    List<String> pathCreated = null;
    do {
      retry = false;
      try {
        _zkClient.create(path, record, mode);
        if (pathCreated == null) {
          pathCreated = new ArrayList<String>();
        }
        pathCreated.add(path);

        return new AccessResult(RetCode.OK, pathCreated, null, null);
      } catch (ZkNoNodeException e) {
        // this will happen if parent node does not exist
        String parentPath = HelixUtil.getZkParentPath(path);
        try {
          AccessResult res = doCreate(parentPath, null, AccessOption.PERSISTENT);
          pathCreated = res._pathCreated;
          RetCode rc = res._retCode;
          if (rc == RetCode.OK || rc == RetCode.NODE_EXISTS) {
            // if parent node created/exists, retry
            retry = true;
          }
        } catch (Exception e1) {
          LOG.error("Exception while creating path: " + parentPath, e1);
          return new AccessResult(RetCode.ERROR, pathCreated, null, null);
        }
      } catch (ZkNodeExistsException e) {
        LOG.warn("Node already exists. path: " + path);
        return new AccessResult(RetCode.NODE_EXISTS);
      } catch (Exception e) {
        LOG.error("Exception while creating path: " + path, e);
        return new AccessResult(RetCode.ERROR);
      }
    } while (retry);

    return new AccessResult(RetCode.OK, pathCreated, null, null);
  }

  /**
   * sync set a znode
   */
  @Override
  public boolean set(String path, T record, int options) {
    return set(path, record, -1, options);
  }

  /**
   * sync set a znode with expect version
   */
  @Override
  public boolean set(String path, T record, int expectVersion, int options) {
    AccessResult result = doSet(path, record, expectVersion, options);
    return result._retCode == RetCode.OK;
  }

  /**
   * sync set a znode, create parent paths if necessary
   * @param path
   * @param record
   * @param expectVersion
   * @param options
   */
  public AccessResult doSet(String path, T record, int expectVersion, int options) {
    if (path == null) {
      throw new NullPointerException("path can't be null");
    }

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      throw new IllegalArgumentException("Invalid set options: " + options);
    }

    Stat stat = null;
    List<String> pathCreated = null;
    boolean retry;
    do {
      retry = false;
      try {
        stat = _zkClient.writeDataGetStat(path, record, expectVersion);
      } catch (ZkNoNodeException e) {
        // node not exists, try create if expectedVersion == -1; in this case, stat will not be set
        if (expectVersion != -1) {
          LOG.error("Could not create node if expectVersion != -1, was " + expectVersion);
          return new AccessResult(RetCode.ERROR);
        }
        try {
          // may create recursively
          AccessResult res = doCreate(path, record, options);
          pathCreated = res._pathCreated;
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
            return new AccessResult(RetCode.ERROR, pathCreated, null, null);
          }
        } catch (Exception e1) {
          LOG.error("Exception while setting path by creating: " + path, e);
          return new AccessResult(RetCode.ERROR, pathCreated, null, null);
        }
      } catch (ZkBadVersionException e) {
        throw e;
      } catch (Exception e) {
        LOG.error("Exception while setting path: " + path, e);
        return new AccessResult(RetCode.ERROR, pathCreated, null, null);
      }
    } while (retry);

    return new AccessResult(RetCode.OK, pathCreated, stat, null);
  }

  /**
   * sync update a znode
   */
  @Override
  public boolean update(String path, DataUpdater<T> updater, int options) {
    AccessResult result = doUpdate(path, updater, options);
    return result._retCode == RetCode.OK;
  }

  /**
   * sync update a znode, create parent paths if necessary
   * @param path
   * @param updater
   * @param options
   */
  public AccessResult doUpdate(String path, DataUpdater<T> updater, int options) {
    if (path == null || updater == null) {
      throw new NullPointerException("path|updater can't be null");
    }

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      throw new IllegalArgumentException("Invalid update options: " + options);
    }

    boolean retry;
    Stat setStat = null;
    T updatedData = null;
    List<String> pathCreated = null;
    do {
      retry = false;
      try {
        Stat readStat = new Stat();
        T oldData = (T) _zkClient.readData(path, readStat);
        T newData = updater.update(oldData);
        setStat = _zkClient.writeDataGetStat(path, newData, readStat.getVersion());

        updatedData = newData;
      } catch (ZkBadVersionException e) {
        retry = true;
      } catch (ZkNoNodeException e) {
        // node not exist, try create, pass null to updater
        try {
          T newData = updater.update(null);
          AccessResult res = doCreate(path, newData, options);
          pathCreated = res._pathCreated;
          RetCode rc = res._retCode;
          switch (rc) {
          case OK:
            updatedData = newData;
            break;
          case NODE_EXISTS:
            retry = true;
            break;
          default:
            LOG.error("Fail to update path by creating: " + path);
            return new AccessResult(RetCode.ERROR, pathCreated, null, null);
          }
        } catch (Exception e1) {
          LOG.error("Exception while updating path by creating: " + path, e1);
          return new AccessResult(RetCode.ERROR, pathCreated, null, null);
        }
      } catch (Exception e) {
        LOG.error("Exception while updating path: " + path, e);
        return new AccessResult(RetCode.ERROR, pathCreated, null, null);
      }
    } while (retry);

    return new AccessResult(RetCode.OK, pathCreated, setStat, updatedData);
  }

  /**
   * sync get a znode
   */
  @Override
  public T get(String path, Stat stat, int options) {
    if (path == null) {
      throw new NullPointerException("path can't be null");
    }

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
   * async get a list of znodes
   */
  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options) {
    if (paths == null) {
      throw new NullPointerException("paths can't be null");
    }

    if (stats != null && stats.size() > 0) {
      throw new IllegalArgumentException(
          "stats list is an output parameter and should be empty, but was: " + stats);
    }

    boolean[] needRead = new boolean[paths.size()];
    Arrays.fill(needRead, true);

    List<AccessResult> accessResults = doGet(paths, needRead);
    List<T> values = new ArrayList<T>();

    for (AccessResult accessResult : accessResults) {
      values.add(accessResult._resultValue);
      if (stats != null) {
        stats.add(accessResult._stat);
      }
    }

    return values;
  }

  /**
   * async get a list of znodes
   */
  List<AccessResult> doGet(List<String> paths, boolean[] needRead) {
    if (paths == null || needRead == null) {
      throw new NullPointerException("paths|needRead can't be null");
    }

    final int size = paths.size();
    if (size != needRead.length) {
      throw new IllegalArgumentException(
          "paths and needRead should of equal size, but paths size: " + size + ", needRead size: "
              + needRead.length);
    }

    for (int i = 0; i < size; i++) {
      if (!needRead[i]) {
        continue;
      }

      if (paths.get(i) == null) {
        throw new NullPointerException("path[" + i + "] can't be null, but was: " + paths);
      }
    }

    if (size == 0) {
      return Collections.emptyList();
    }

    // init all results to null
    List<AccessResult> results =
        new ArrayList<AccessResult>(Collections.<AccessResult> nCopies(size, null));

    long startT = System.nanoTime();

    try {
      // issue asyn get requests
      GetDataCallbackHandler[] cbList = new GetDataCallbackHandler[size];
      for (int i = 0; i < size; i++) {
        if (!needRead[i]) {
          continue;
        }

        String path = paths.get(i);
        cbList[i] = new GetDataCallbackHandler();
        _zkClient.asyncGetData(path, cbList[i]);
      }

      // wait for completion
      for (int i = 0; i < size; i++) {
        if (!needRead[i]) {
          continue;
        }

        GetDataCallbackHandler cb = cbList[i];
        cb.waitForSuccess();
      }

      // construct return results
      for (int i = 0; i < paths.size(); i++) {
        if (!needRead[i]) {
          continue;
        }

        GetDataCallbackHandler cb = cbList[i];
        switch (Code.get(cb.getRc())) {
        case OK: {
          @SuppressWarnings("unchecked")
          T value = (T) _zkClient.deserialize(cb._data, paths.get(i));
          results.set(i, new AccessResult(RetCode.OK, null, cb._stat, value));
          break;
        }
        case NONODE: {
          results.set(i, new AccessResult(RetCode.NONODE));
          break;
        }
        default: {
          results.set(i, new AccessResult(RetCode.ERROR));
          break;
        }
        }
      }

      return results;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("getData_async, size: " + size + ", paths: " + paths + ", time: "
            + (endT - startT) + " ns");
      }
    }
  }

  /**
   * asyn getChildren
   */
  @Override
  public List<T> getChildren(String parentPath, List<Stat> stats, int options) {
    if (stats != null && stats.size() > 0) {
      throw new IllegalArgumentException(
          "stats list is an output parameter and should be empty, but was: " + stats);
    }

    try {
      // prepare child paths
      List<String> childNames = getChildNames(parentPath, options);
      if (childNames == null || childNames.size() == 0) {
        return Collections.emptyList();
      }

      List<String> paths = new ArrayList<String>();
      for (String childName : childNames) {
        String path = parentPath + "/" + childName;
        paths.add(path);
      }

      boolean[] needRead = new boolean[paths.size()];
      Arrays.fill(needRead, true);

      List<AccessResult> results = doGet(paths, needRead);
      List<T> values = new ArrayList<T>();
      for (AccessResult result : results) {
        if (result._retCode == RetCode.OK) {
          values.add(result._resultValue);
          if (stats != null) {
            stats.add(result._stat);
          }
        }
      }

      return values;
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
   * sync remove
   */
  @Override
  public boolean remove(String path, int options) {
    try {
      // optimize on common path
      return _zkClient.delete(path);
    } catch (ZkException e) {
      return _zkClient.deleteRecursive(path);
    }
  }

  /**
   * async create. give up on error other than NONODE
   */
  List<AccessResult> doCreate(List<String> paths, List<T> records, boolean[] needCreate, int options) {
    if (paths == null) {
      throw new NullPointerException("paths can't be null");
    }

    for (int i = 0; i < paths.size(); i++) {
      if (!needCreate[i]) {
        continue;
      }

      if (paths.get(i) == null) {
        throw new NullPointerException("path[" + i + "] can't be null, but was: " + paths);
      }
    }

    if (records != null && records.size() != paths.size()) {
      throw new IllegalArgumentException(
          "paths and records should be of same size, but paths size: " + paths.size()
              + ", records size: " + records.size());
    }

    if (needCreate == null) {
      throw new NullPointerException("needCreate can't be null");
    }

    if (needCreate.length != paths.size()) {
      throw new IllegalArgumentException(
          "paths and needCreate should be of same size, but paths size: " + paths.size()
              + ", needCreate size: " + needCreate.length);
    }

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      throw new IllegalArgumentException("Invalid async set options: " + options);
    }

    CreateCallbackHandler[] cbList = new CreateCallbackHandler[paths.size()];
    List<List<String>> pathsCreated =
        new ArrayList<List<String>>(Collections.<List<String>> nCopies(paths.size(), null));
    RetCode retCodes[] = new RetCode[paths.size()];

    boolean retry;
    do {
      retry = false;

      for (int i = 0; i < paths.size(); i++) {
        if (!needCreate[i]) {
          continue;
        }

        String path = paths.get(i);
        T record = (records == null ? null : records.get(i));
        cbList[i] = new CreateCallbackHandler();

        _zkClient.asyncCreate(path, record, mode, cbList[i]);
      }

      List<String> parentPaths =
          new ArrayList<String>(Collections.<String> nCopies(paths.size(), null));
      boolean failOnNoParentNode = false;

      for (int i = 0; i < paths.size(); i++) {
        if (!needCreate[i]) {
          continue;
        }

        CreateCallbackHandler cb = cbList[i];
        cb.waitForSuccess();
        String path = paths.get(i);

        Code code = Code.get(cb.getRc());
        switch (code) {
        case NONODE: {
          // we will try create parent nodes
          String parentPath = HelixUtil.getZkParentPath(path);
          parentPaths.set(i, parentPath);
          failOnNoParentNode = true;
          break;
        }
        case NODEEXISTS: {
          retCodes[i] = RetCode.NODE_EXISTS;
          needCreate[i] = false;
          break;
        }
        case OK: {
          retCodes[i] = RetCode.OK;
          if (pathsCreated.get(i) == null) {
            pathsCreated.set(i, new ArrayList<String>());
          }
          pathsCreated.get(i).add(path);
          needCreate[i] = false;
          break;
        }
        default: {
          retCodes[i] = RetCode.ERROR;
          needCreate[i] = false;
          break;
        }
        }
      }

      if (failOnNoParentNode) {
        List<AccessResult> createParentResults =
            doCreate(parentPaths, null, Arrays.copyOf(needCreate, needCreate.length),
                AccessOption.PERSISTENT);
        for (int i = 0; i < createParentResults.size(); i++) {
          if (!needCreate[i]) {
            continue;
          }

          // if parent is created, retry create child
          AccessResult result = createParentResults.get(i);
          pathsCreated.set(i, result._pathCreated);

          if (result._retCode == RetCode.OK || result._retCode == RetCode.NODE_EXISTS) {
            retry = true;
          } else {
            retCodes[i] = RetCode.ERROR;
            needCreate[i] = false;
          }
        }
      }
    } while (retry);

    List<AccessResult> results = new ArrayList<AccessResult>();
    for (int i = 0; i < paths.size(); i++) {
      results.add(new AccessResult(retCodes[i], pathsCreated.get(i), null, null));
    }
    return results;
  }

  // TODO: rename to create
  /**
   * async create multiple znodes
   */
  @Override
  public boolean[] createChildren(List<String> paths, List<T> records, int options) {
    boolean[] success = new boolean[paths.size()];

    boolean[] needCreate = new boolean[paths.size()];
    Arrays.fill(needCreate, true);

    long startT = System.nanoTime();
    try {
      List<AccessResult> results = doCreate(paths, records, needCreate, options);

      for (int i = 0; i < paths.size(); i++) {
        AccessResult result = results.get(i);
        success[i] = (result._retCode == RetCode.OK);
      }

      return success;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("create_async, size: " + paths.size() + ", paths: " + paths + ", time: "
            + (endT - startT) + " ns");
      }
    }
  }

  // TODO: rename to set
  /**
   * async set multiple znodes
   */
  @Override
  public boolean[] setChildren(List<String> paths, List<T> records, int options) {
    List<AccessResult> results = doSet(paths, records, options);
    boolean[] success = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      success[i] = (results.get(i)._retCode == RetCode.OK);
    }

    return success;
  }

  /**
   * async set, give up on error other than NoNode
   */
  List<AccessResult> doSet(List<String> paths, List<T> records, int options) {
    if (paths == null) {
      throw new NullPointerException("paths can't be null");
    }

    for (String path : paths) {
      if (path == null) {
        throw new NullPointerException("path can't be null, but was: " + paths);
      }
    }

    final int size = paths.size();
    if (records != null && records.size() != size) {
      throw new IllegalArgumentException(
          "paths and records should be of same size, but paths size: " + size + ", records size: "
              + records.size());
    }

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      throw new IllegalArgumentException("Invalid async set options: " + options);
    }

    if (size == 0) {
      return Collections.emptyList();
    }

    Stat[] setStats = new Stat[size];
    RetCode[] retCodes = new RetCode[size];
    List<List<String>> pathsCreated =
        new ArrayList<List<String>>(Collections.<List<String>> nCopies(size, null));

    SetDataCallbackHandler[] setCbList = new SetDataCallbackHandler[size];

    boolean[] needSet = new boolean[size];
    Arrays.fill(needSet, true);

    long startT = System.nanoTime();

    try {
      boolean retry;
      do {
        retry = false;

        for (int i = 0; i < size; i++) {
          if (!needSet[i]) {
            continue;
          }

          String path = paths.get(i);
          T record = (records == null ? null : records.get(i));
          setCbList[i] = new SetDataCallbackHandler();

          _zkClient.asyncSetData(path, record, -1, setCbList[i]);
        }

        boolean failOnNoNode = false;

        for (int i = 0; i < size; i++) {
          if (!needSet[i]) {
            continue;
          }

          SetDataCallbackHandler cb = setCbList[i];
          cb.waitForSuccess();
          Code rc = Code.get(cb.getRc());
          switch (rc) {
          case OK: {
            setStats[i] = cb.getStat();
            retCodes[i] = RetCode.OK;
            needSet[i] = false;
            break;
          }
          case NONODE: {
            // if fail on NoNode, try create the node
            failOnNoNode = true;
            break;
          }
          default: {
            // if fail on error other than NoNode, give up
            retCodes[i] = RetCode.ERROR;
            needSet[i] = false;
            break;
          }
          }
        }

        // if failOnNoNode, try create
        if (failOnNoNode) {
          List<AccessResult> createResults =
              doCreate(paths, records, Arrays.copyOf(needSet, size), options);
          for (int i = 0; i < size; i++) {
            if (!needSet[i]) {
              continue;
            }

            AccessResult createResult = createResults.get(i);
            RetCode code = createResult._retCode;
            pathsCreated.set(i, createResult._pathCreated);

            switch (code) {
            case OK: {
              setStats[i] = ZNode.ZERO_STAT;
              retCodes[i] = RetCode.OK;
              needSet[i] = false;
              break;
            }
            case NODE_EXISTS: {
              retry = true;
              break;
            }
            default: {
              // creation fails on error other than NodeExists
              retCodes[i] = RetCode.ERROR;
              needSet[i] = false;
              break;
            }
            }
          }
        }
      } while (retry);

      // construct return results
      List<AccessResult> results = new ArrayList<AccessResult>();
      for (int i = 0; i < size; i++) {
        results.add(new AccessResult(retCodes[i], pathsCreated.get(i), setStats[i], null));
      }

      return results;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("setData_async, size: " + size + ", paths: " + paths + ", time: "
            + (endT - startT) + " ns");
      }
    }
  }

  // TODO: rename to update
  /**
   * async update
   */
  @Override
  public boolean[] updateChildren(List<String> paths, List<DataUpdater<T>> updaters, int options) {

    List<AccessResult> results = doUpdate(paths, updaters, options);
    boolean[] success = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      success[i] = (results.get(i)._retCode == RetCode.OK);
    }

    return success;
  }

  /**
   * async update multiple znodes
   */
  List<AccessResult> doUpdate(List<String> paths, List<DataUpdater<T>> updaters, int options) {
    if (paths == null || updaters == null) {
      throw new NullPointerException("paths|updaters can't be null");
    }

    for (String path : paths) {
      if (path == null) {
        throw new NullPointerException("path can't be null, but was: " + paths);
      }
    }

    for (DataUpdater<T> updater : updaters) {
      if (updater == null) {
        throw new NullPointerException("updater can't be null, but was: " + updaters + ", paths: "
            + paths);
      }
    }

    final int size = paths.size();
    if (updaters.size() != size) {
      throw new IllegalArgumentException(
          "paths and updaters should be of same size, but paths size: " + size
              + ", updaters size: " + updaters.size());
    }

    CreateMode mode = AccessOption.getMode(options);
    if (mode == null) {
      throw new IllegalArgumentException("Invalid update options: " + options);
    }

    if (size == 0) {
      return Collections.emptyList();
    }

    Stat[] updateStats = new Stat[size];
    RetCode[] retCodes = new RetCode[size];
    List<List<String>> pathsCreated =
        new ArrayList<List<String>>(Collections.<List<String>> nCopies(size, null));
    List<T> updateData = new ArrayList<T>(Collections.<T> nCopies(size, null));

    boolean[] needUpdate = new boolean[size];
    Arrays.fill(needUpdate, true);

    long startT = System.nanoTime();

    try {
      boolean retry;
      do {
        retry = false;
        SetDataCallbackHandler[] setCbList = new SetDataCallbackHandler[size];
        boolean[] needCreate = new boolean[size]; // init'ed with false
        boolean failOnNoNode = false;

        // asycn read all data
        List<AccessResult> readResults = doGet(paths, Arrays.copyOf(needUpdate, size));

        // async update
        List<T> newDataList = new ArrayList<T>(Collections.<T> nCopies(size, null));
        for (int i = 0; i < size; i++) {
          if (!needUpdate[i]) {
            continue;
          }
          String path = paths.get(i);
          DataUpdater<T> updater = updaters.get(i);
          AccessResult readResult = readResults.get(i);
          T newData = updater.update(readResult._resultValue);
          newDataList.set(i, newData);
          if (readResult._retCode == RetCode.NONODE) {
            // node not exists
            failOnNoNode = true;
            needCreate[i] = true;
          } else {
            setCbList[i] = new SetDataCallbackHandler();
            _zkClient.asyncSetData(path, newData, readResult._stat.getVersion(), setCbList[i]);
          }
        }

        // wait for completion
        boolean failOnBadVersion = false;

        for (int i = 0; i < size; i++) {
          SetDataCallbackHandler cb = setCbList[i];
          if (cb == null) {
            continue;
          }

          cb.waitForSuccess();

          switch (Code.get(cb.getRc())) {
          case OK: {
            updateData.set(i, newDataList.get(i));
            updateStats[i] = cb.getStat();
            retCodes[i] = RetCode.OK;
            needUpdate[i] = false;
            break;
          }
          case NONODE: {
            failOnNoNode = true;
            needCreate[i] = true;
            break;
          }
          case BADVERSION: {
            failOnBadVersion = true;
            break;
          }
          default: {
            // fail on error other than NoNode or BadVersion
            needUpdate[i] = false;
            retCodes[i] = RetCode.ERROR;
            break;
          }
          }
        }

        // if failOnNoNode, try create
        if (failOnNoNode) {
          List<AccessResult> createResults =
              doCreate(paths, newDataList, Arrays.copyOf(needCreate, size), options);
          for (int i = 0; i < size; i++) {
            if (!needCreate[i]) {
              continue;
            }

            AccessResult result = createResults.get(i);
            pathsCreated.set(i, result._pathCreated);

            switch (result._retCode) {
            case OK: {
              needUpdate[i] = false;
              updateData.set(i, newDataList.get(i));
              updateStats[i] = ZNode.ZERO_STAT;
              retCodes[i] = RetCode.OK;
              break;
            }
            case NODE_EXISTS: {
              retry = true;
              break;
            }
            default: {
              // fail on error other than NodeExists
              retCodes[i] = RetCode.ERROR;
              needUpdate[i] = false;
              break;
            }
            }
          }
        }

        // if failOnBadVersion, retry
        if (failOnBadVersion) {
          retry = true;
        }
      } while (retry);

      List<AccessResult> results = new ArrayList<AccessResult>();
      for (int i = 0; i < size; i++) {
        results.add(new AccessResult(retCodes[i], pathsCreated.get(i), updateStats[i], updateData
            .get(i)));
      }
      return results;
    } finally {
      long endT = System.nanoTime();
      if (LOG.isTraceEnabled()) {
        LOG.trace("updateData_async, size: " + size + ", paths: " + paths + ", time: "
            + (endT - startT) + " ns");
      }
    }

  }

  /**
   * async test existence on multiple znodes
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
   * async get stats of mulitple znodes
   */
  @Override
  public Stat[] getStats(List<String> paths, int options) {
    if (paths == null) {
      throw new NullPointerException("paths can't be null");
    }

    if (paths.size() == 0) {
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
        LOG.trace("exists_async, size: " + paths.size() + ", paths: " + paths + ", time: "
            + (endT - startT) + " ns");
      }
    }
  }

  /**
   * async remove multiple znodes
   */
  @Override
  public boolean[] remove(List<String> paths, int options) {
    if (paths == null) {
      throw new NullPointerException("paths can't be null");
    }

    if (paths.size() == 0) {
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
        LOG.trace("delete_async, size: " + paths.size() + ", paths: " + paths + ", time: "
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
