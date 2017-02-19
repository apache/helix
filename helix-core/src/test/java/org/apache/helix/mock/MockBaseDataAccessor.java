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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.zookeeper.data.Stat;

public class MockBaseDataAccessor implements BaseDataAccessor<ZNRecord> {
  Map<String, ZNRecord> map = new HashMap<String, ZNRecord>();

  @Override public boolean create(String path, ZNRecord record, int options) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override public boolean set(String path, ZNRecord record, int options) {
    System.err.println("Store.write()" + System.currentTimeMillis());
    map.put(path, record);
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return true;
  }

  @Override public boolean update(String path, DataUpdater<ZNRecord> updater, int options) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override public boolean remove(String path, int options) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override public boolean[] createChildren(List<String> paths, List<ZNRecord> records,
      int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public boolean[] setChildren(List<String> paths, List<ZNRecord> records, int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public boolean[] updateChildren(List<String> paths,
      List<DataUpdater<ZNRecord>> updaters, int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public boolean[] remove(List<String> paths, int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public ZNRecord get(String path, Stat stat, int options) {
    return map.get(path);
  }

  @Override public List<ZNRecord> get(List<String> paths, List<Stat> stats, int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public List<String> getChildNames(String parentPath, int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public boolean exists(String path, int options) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override public boolean[] exists(List<String> paths, int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public Stat[] getStats(List<String> paths, int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public Stat getStat(String path, int options) {
    // TODO Auto-generated method stub
    return null;
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
    // TODO Auto-generated method stub

  }

  @Override public boolean set(String path, ZNRecord record, int options, int expectVersion) {
    // TODO Auto-generated method stub
    return false;
  }

}
