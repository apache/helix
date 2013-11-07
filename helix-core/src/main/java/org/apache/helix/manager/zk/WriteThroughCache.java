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

import java.util.List;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.store.zk.ZNode;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

public class WriteThroughCache<T> extends Cache<T> {
  private static Logger LOG = Logger.getLogger(WriteThroughCache.class);

  final BaseDataAccessor<T> _accessor;

  public WriteThroughCache(BaseDataAccessor<T> accessor, List<String> paths) {
    super();
    _accessor = accessor;

    // init cache
    if (paths != null && !paths.isEmpty()) {
      for (String path : paths) {
        updateRecursive(path);
      }
    }
  }

  @Override
  public void update(String path, T data, Stat stat) {
    String parentPath = HelixUtil.getZkParentPath(path);
    String childName = HelixUtil.getZkName(path);
    addToParentChildSet(parentPath, childName);

    ZNode znode = _cache.get(path);
    if (znode == null) {
      _cache.put(path, new ZNode(path, data, stat));
    } else {
      znode.setData(data);
      znode.setStat(stat);
    }
  }

  @Override
  public void updateRecursive(String path) {
    if (path == null) {
      return;
    }

    try {
      _lock.writeLock().lock();

      // update this node
      Stat stat = new Stat();
      T readData = _accessor.get(path, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);

      update(path, readData, stat);

      // recursively update children nodes if not exists
      ZNode znode = _cache.get(path);
      List<String> childNames = _accessor.getChildNames(path, 0);
      if (childNames != null && childNames.size() > 0) {
        for (String childName : childNames) {
          String childPath = path + "/" + childName;
          if (!znode.hasChild(childName)) {
            znode.addChild(childName);
            updateRecursive(childPath);
          }
        }
      }
    } catch (ZkNoNodeException e) {
      // OK. someone delete znode while we are updating cache
    } finally {
      _lock.writeLock().unlock();
    }
  }
}
