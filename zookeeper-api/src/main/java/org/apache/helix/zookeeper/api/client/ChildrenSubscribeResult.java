package org.apache.helix.zookeeper.api.client;
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

import java.util.Collections;
import java.util.List;

import org.apache.helix.zookeeper.zkclient.IZkChildListener;


/** Represents return type of {@link org.apache.helix.zookeeper.api.client.RealmAwareZkClient#subscribeChildChanges(String, IZkChildListener, boolean)}
 *  The returned value would signal if watch installation to ZooKeeper server succeeded
 *  or not using field _isInstalled. The _children field would contains the list of child names
 *  of the watched path. It would be null if the parent path does not exist at the time of watch
 *  installation.
 */
public class ChildrenSubscribeResult {
  private final List<String> _children;
  private final boolean _isInstalled;

  public ChildrenSubscribeResult(List<String> children, boolean isInstalled) {
    if (children != null) {
      _children = Collections.unmodifiableList(children);
    } else {
      _children = null;
    }
    _isInstalled = isInstalled;
  }

  public List<String> getChildren() {
    return _children;
  }

  public boolean isInstalled() {
    return _isInstalled;
  }
}
