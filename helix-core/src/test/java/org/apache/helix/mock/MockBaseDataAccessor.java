package org.apache.helix.mock;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.zookeeper.data.Stat;

public class MockBaseDataAccessor implements BaseDataAccessor<ZNRecord> {

  class ZNode {
    private ZNRecord _record;
    private Stat _stat;

    public ZNode (ZNRecord record) {
      _record = record;
      _stat = new Stat();
      _stat.setCtime(System.currentTimeMillis());
    }

    public void set (ZNRecord record) {
      _record = record;
      _stat.setMtime(System.currentTimeMillis());
      _stat.setVersion(_stat.getVersion() + 1);
    }

    public ZNRecord getRecord() {
      return _record;
    }

    public Stat getStat() {
      return _stat;
    }
  }

  Map<String, ZNode> _recordMap = new HashMap<>();

  @Override
  public boolean create(String path, ZNRecord record, int options) {
    return set(path, record, options);
  }

  @Override
  public boolean set(String path, ZNRecord record, int options) {
    ZNode zNode = _recordMap.get(path);
    if (zNode == null) {
      _recordMap.put(path, new ZNode(record));
    } else {
      zNode.set(record);
      _recordMap.put(path, zNode);
    }
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return true;
  }

  @Override
  public boolean update(String path, DataUpdater<ZNRecord> updater, int options) {
    ZNode zNode = _recordMap.get(path);
    ZNRecord current = zNode != null ? zNode.getRecord() : null;
    ZNRecord newRecord = updater.update(current);
    if (newRecord != null) {
      return set(path, newRecord, options);
    }
    return false;
  }

  @Override
  public boolean remove(String path, int options) {
    _recordMap.remove(path);
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return true;
  }

  @Override
  public boolean[] createChildren(List<String> paths, List<ZNRecord> records,
      int options) {
    return setChildren(paths, records, options);
  }

  @Override
  public boolean[] setChildren(List<String> paths, List<ZNRecord> records, int options) {
    boolean [] ret = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      boolean success = create(paths.get(i), records.get(i), options);
      ret[i] = success;
    }
    return ret;
  }

  @Override
  public boolean[] updateChildren(List<String> paths,
      List<DataUpdater<ZNRecord>> updaters, int options) {
    boolean [] ret = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      boolean success = update(paths.get(i), updaters.get(i), options);
      ret[i] = success;
    }
    return ret;
  }

  @Override
  public boolean[] remove(List<String> paths, int options) {
    boolean [] ret = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      boolean success = remove(paths.get(i), options);
      ret[i] = success;
    }
    return ret;
  }

  @Override
  public ZNRecord get(String path, Stat stat, int options) {
    ZNode zNode = _recordMap.get(path);
    return zNode != null ? zNode.getRecord() : null;
  }

  @Override
  public List<ZNRecord> get(List<String> paths, List<Stat> stats, int options) {
    List<ZNRecord> records = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      ZNRecord record = get(paths.get(i), stats.get(i), options);
      records.add(record);
    }
    return records;
  }

  @Override
  public List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options) {
    List<ZNRecord> children = new ArrayList<>();
    for (String key : _recordMap.keySet()) {
      if (key.startsWith(parentPath)) {
        String[] keySplit = key.split("\\/");
        String[] pathSplit = parentPath.split("\\/");
        if (keySplit.length - pathSplit.length == 1) {
          ZNode zNode = _recordMap.get(key);
          ZNRecord record = zNode != null ? zNode.getRecord() : null;
          if (record != null) {
            children.add(record);
          }
        } else {
          System.out.println("keySplit:" + Arrays.toString(keySplit));
          System.out.println("pathSplit:" + Arrays.toString(pathSplit));
        }
      }
    }
    return children;
  }

  @Override
  public List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options,
      int retryCount, int retryInterval) throws HelixException {
    return getChildren(parentPath, stats, options);
  }

  @Override
  public List<String> getChildNames(String parentPath, int options) {
    List<String> child = new ArrayList<>();
    for (String key : _recordMap.keySet()) {
      if (key.startsWith(parentPath)) {
        String[] keySplit = key.split("\\/");
        String[] pathSplit = parentPath.split("\\/");
        if (keySplit.length > pathSplit.length) {
          child.add(keySplit[pathSplit.length]);
        }
      }
    }
    return child;
  }

  @Override
  public boolean exists(String path, int options) {
    return _recordMap.containsKey(path);
  }

  @Override
  public boolean[] exists(List<String> paths, int options) {
    boolean [] ret = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      ret[i] = _recordMap.containsKey(paths.get(i));
    }
    return ret;
  }

  @Override public Stat[] getStats(List<String> paths, int options) {
    Stat [] stats = new Stat[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      stats[i] = getStat(paths.get(i), options);
    }
    return stats;
  }

  @Override public Stat getStat(String path, int options) {
    ZNode zNode = _recordMap.get(path);
    return zNode != null ? zNode.getStat() : null;
  }

  @Override public void subscribeDataChanges(String path, IZkDataListener listener) {
    // TODO Auto-generated method stub

  }

  @Override public void unsubscribeDataChanges(String path, IZkDataListener listener) {
    // TODO Auto-generated method stub

  }

  @Override public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public void unsubscribeChildChanges(String path, IZkChildListener listener) {
    // TODO Auto-generated method stub

  }

  @Override public void reset() {
    _recordMap.clear();
  }

  @Override public boolean set(String path, ZNRecord record, int options, int expectVersion) {
    return set(path, record, options);
  }

}
