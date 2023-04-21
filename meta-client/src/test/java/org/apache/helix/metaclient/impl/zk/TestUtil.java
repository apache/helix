package org.apache.helix.metaclient.impl.zk;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.helix.zookeeper.zkclient.ZkConnection;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


public class TestUtil {

  static java.lang.reflect.Field getField(Class clazz, String fieldName)
      throws NoSuchFieldException {
    try {
      return clazz.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      Class superClass = clazz.getSuperclass();
      if (superClass == null) {
        throw e;
      }
      return getField(superClass, fieldName);
    }
  }

  public static Map<String, List<String>> getZkWatch(RealmAwareZkClient client)
      throws Exception {
    Map<String, List<String>> lists = new HashMap<String, List<String>>();
    ZkClient zkClient = (ZkClient) client;

    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper zk = connection.getZookeeper();

    java.lang.reflect.Field field = getField(zk.getClass(), "watchManager");
    field.setAccessible(true);
    Object watchManager = field.get(zk);

    java.lang.reflect.Field field2 = getField(watchManager.getClass(), "dataWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> dataWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);

    field2 = getField(watchManager.getClass(), "existWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> existWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);

    field2 = getField(watchManager.getClass(), "childWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> childWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);

    field2 = getField(watchManager.getClass(), "persistentWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> persistentWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);

    field2 = getField(watchManager.getClass(), "persistentRecursiveWatches");
    field2.setAccessible(true);
    HashMap<String, Set<Watcher>> persistentRecursiveWatches =
        (HashMap<String, Set<Watcher>>) field2.get(watchManager);


    lists.put("dataWatches", new ArrayList<>(dataWatches.keySet()));
    lists.put("existWatches", new ArrayList<>(existWatches.keySet()));
    lists.put("childWatches", new ArrayList<>(childWatches.keySet()));
    lists.put("persistentWatches", new ArrayList<>(persistentWatches.keySet()));
    lists.put("persistentRecursiveWatches", new ArrayList<>(persistentRecursiveWatches.keySet()));

    return lists;
  }
}