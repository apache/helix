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

import java.util.List;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkCacheBaseDataAccessor;

public class ZkHelixPropertyStore<T> extends ZkCacheBaseDataAccessor<T> {
  public ZkHelixPropertyStore(ZkBaseDataAccessor<T> accessor, String root,
      List<String> subscribedPaths) {
    super(accessor, root, null, subscribedPaths);
  }

  public ZkHelixPropertyStore(String zkAddress, ZkSerializer serializer, String chrootPath,
      List<String> zkCachePaths) {
    super(zkAddress, serializer, chrootPath, null, zkCachePaths);
  }

  public ZkHelixPropertyStore(String zkAddress, ZkSerializer serializer, String chrootPath) {
    super(zkAddress, serializer, chrootPath, null, null);
  }
}
