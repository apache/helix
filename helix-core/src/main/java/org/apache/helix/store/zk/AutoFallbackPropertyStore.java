package org.apache.helix.store.zk;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

/**
 * Property store that does auto fallback to an old location.
 * Assuming no concurrent updates
 */
public class AutoFallbackPropertyStore<T> extends ZkHelixPropertyStore<T> {
  private static Logger LOG = Logger.getLogger(AutoFallbackPropertyStore.class);

  private final ZkHelixPropertyStore<T> _fallbackStore;

  public AutoFallbackPropertyStore(ZkBaseDataAccessor<T> accessor, String root, String fallbackRoot) {
    super(accessor, root, null);

    if (accessor.exists(fallbackRoot, 0)) {
      _fallbackStore = new ZkHelixPropertyStore<T>(accessor, fallbackRoot, null);
    } else {
      LOG.info("fallbackRoot: " + fallbackRoot
          + " doesn't exist, skip creating fallback property store");
      _fallbackStore = null;
    }

  }

  @Override
  public boolean update(String path, DataUpdater<T> updater, int options) {
    if (_fallbackStore == null) {
      return super.update(path, updater, options);
    } else {
      Stat stat = super.getStat(path, options);
      if (stat == null) {
        // create znode at new location with fallback-value
        T fallbackValue = _fallbackStore.get(path, null, options);
        boolean succeed = super.create(path, fallbackValue, AccessOption.PERSISTENT);
        if (!succeed) {
          LOG.error("Can't update " + path + " since there are concurrent updates");
          return false;
        }
      }
      return super.update(path, updater, options);
    }
  }

  @Override
  public boolean exists(String path, int options) {
    if (_fallbackStore == null) {
      return super.exists(path, options);
    } else {
      boolean exist = super.exists(path, options);
      if (!exist) {
        exist = _fallbackStore.exists(path, options);
      }
      return exist;
    }
  }

  @Override
  public boolean remove(String path, int options) {
    if (_fallbackStore != null) {
      _fallbackStore.remove(path, options);
    }
    return super.remove(path, options);
  }

  @Override
  public T get(String path, Stat stat, int options) {
    if (_fallbackStore == null) {
      return super.get(path, stat, options);
    } else {
      T value = super.get(path, stat, options);
      if (value == null) {
        value = _fallbackStore.get(path, stat, options);
      }

      return value;
    }
  }

  @Override
  public Stat getStat(String path, int options) {
    if (_fallbackStore == null) {
      return super.getStat(path, options);
    } else {
      Stat stat = super.getStat(path, options);

      if (stat == null) {
        stat = _fallbackStore.getStat(path, options);
      }
      return stat;
    }
  }

  @Override
  public boolean[] updateChildren(List<String> paths, List<DataUpdater<T>> updaters, int options) {
    if (_fallbackStore == null) {
      return super.updateChildren(paths, updaters, options);
    } else {
      Stat[] stats = super.getStats(paths, options);
      Map<String, Integer> fallbackMap = new HashMap<String, Integer>();
      Map<String, Integer> updateMap = new HashMap<String, Integer>();
      for (int i = 0; i < paths.size(); i++) {
        String path = paths.get(i);
        if (stats[i] == null) {
          fallbackMap.put(path, i);
        } else {
          updateMap.put(path, i);
        }
      }

      if (fallbackMap.size() > 0) {
        List<String> fallbackPaths = new ArrayList<String>(fallbackMap.keySet());
        List<T> fallbackValues = _fallbackStore.get(fallbackPaths, null, options);
        boolean createSucceed[] =
            super.createChildren(fallbackPaths, fallbackValues, AccessOption.PERSISTENT);

        for (int i = 0; i < fallbackPaths.size(); i++) {
          String fallbackPath = fallbackPaths.get(i);
          if (createSucceed[i]) {
            updateMap.put(fallbackPath, fallbackMap.get(fallbackPath));
          } else {
            LOG.error("Can't update " + fallbackPath + " since there are concurrent updates");
          }
        }
      }

      boolean succeed[] = new boolean[paths.size()]; // all init'ed to false
      if (updateMap.size() > 0) {
        List<String> updatePaths = new ArrayList<String>(updateMap.keySet());
        List<DataUpdater<T>> subUpdaters = new ArrayList<DataUpdater<T>>();
        for (int i = 0; i < updatePaths.size(); i++) {
          String updatePath = updatePaths.get(i);
          subUpdaters.add(updaters.get(updateMap.get(updatePath)));
        }

        boolean updateSucceed[] = super.updateChildren(updatePaths, subUpdaters, options);
        for (int i = 0; i < updatePaths.size(); i++) {
          String updatePath = updatePaths.get(i);
          if (updateSucceed[i]) {
            succeed[updateMap.get(updatePath)] = true;
          }
        }
      }

      return succeed;
    }
  }

  @Override
  public boolean[] exists(List<String> paths, int options) {
    if (_fallbackStore == null) {
      return super.exists(paths, options);
    } else {
      boolean[] exists = super.exists(paths, options);

      Map<String, Integer> fallbackMap = new HashMap<String, Integer>();
      for (int i = 0; i < paths.size(); i++) {
        boolean exist = exists[i];
        if (!exist) {
          fallbackMap.put(paths.get(i), i);
        }
      }

      if (fallbackMap.size() > 0) {
        List<String> fallbackPaths = new ArrayList<String>(fallbackMap.keySet());

        boolean[] fallbackExists = _fallbackStore.exists(fallbackPaths, options);
        for (int i = 0; i < fallbackPaths.size(); i++) {
          String fallbackPath = fallbackPaths.get(i);
          int j = fallbackMap.get(fallbackPath);
          exists[j] = fallbackExists[i];
        }
      }

      return exists;
    }
  }

  @Override
  public boolean[] remove(List<String> paths, int options) {
    if (_fallbackStore != null) {
      _fallbackStore.remove(paths, options);
    }
    return super.remove(paths, options);
  }

  @Override
  public List<T> get(List<String> paths, List<Stat> stats, int options) {
    if (_fallbackStore == null) {
      return super.get(paths, stats, options);
    } else {
      List<T> values = super.get(paths, stats, options);

      Map<String, Integer> fallbackMap = new HashMap<String, Integer>();
      for (int i = 0; i < paths.size(); i++) {
        T value = values.get(i);
        if (value == null) {
          fallbackMap.put(paths.get(i), i);
        }
      }

      if (fallbackMap.size() > 0) {
        List<String> fallbackPaths = new ArrayList<String>(fallbackMap.keySet());
        List<Stat> fallbackStats = new ArrayList<Stat>();
        List<T> fallbackValues = _fallbackStore.get(fallbackPaths, fallbackStats, options);
        for (int i = 0; i < fallbackPaths.size(); i++) {
          String fallbackPath = fallbackPaths.get(i);
          int j = fallbackMap.get(fallbackPath);
          values.set(j, fallbackValues.get(i));
          if (stats != null) {
            stats.set(j, fallbackStats.get(i));
          }
        }
      }

      return values;
    }
  }

  @Override
  public Stat[] getStats(List<String> paths, int options) {
    if (_fallbackStore == null) {
      return super.getStats(paths, options);
    } else {
      Stat[] stats = super.getStats(paths, options);

      Map<String, Integer> fallbackMap = new HashMap<String, Integer>();
      for (int i = 0; i < paths.size(); i++) {
        Stat stat = stats[i];
        if (stat == null) {
          fallbackMap.put(paths.get(i), i);
        }
      }

      if (fallbackMap.size() > 0) {
        List<String> fallbackPaths = new ArrayList<String>(fallbackMap.keySet());

        Stat[] fallbackStats = _fallbackStore.getStats(fallbackPaths, options);
        for (int i = 0; i < fallbackPaths.size(); i++) {
          String fallbackPath = fallbackPaths.get(i);
          int j = fallbackMap.get(fallbackPath);
          stats[j] = fallbackStats[i];
        }
      }

      return stats;
    }
  }

  @Override
  public List<String> getChildNames(String parentPath, int options) {
    if (_fallbackStore == null) {
      return super.getChildNames(parentPath, options);
    } else {
      List<String> childs = super.getChildNames(parentPath, options);
      List<String> fallbackChilds = _fallbackStore.getChildNames(parentPath, options);

      if (childs == null && fallbackChilds == null) {
        return null;
      }

      // merge two child lists
      Set<String> allChildSet = new HashSet<String>();
      if (childs != null) {
        allChildSet.addAll(childs);
      }

      if (fallbackChilds != null) {
        allChildSet.addAll(fallbackChilds);
      }

      List<String> allChilds = new ArrayList<String>(allChildSet);
      return allChilds;
    }
  }

  @Override
  public void start() {
    if (_fallbackStore != null) {
      _fallbackStore.start();
    }
  }

  @Override
  public void stop() {
    if (_fallbackStore != null) {
      _fallbackStore.stop();
    }

    super.stop();
  }

  @Override
  public void reset() {
    if (_fallbackStore != null) {
      _fallbackStore.reset();
    }

    super.reset();
  }
}
