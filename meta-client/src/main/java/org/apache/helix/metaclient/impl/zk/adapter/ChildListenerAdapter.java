package org.apache.helix.metaclient.impl.zk.adapter;

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

import java.util.List;
import org.apache.helix.metaclient.api.ChildChangeListener;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.RecursivePersistListener;
import org.apache.zookeeper.Watcher;


/**
 * A adapter class to transform {@link ChildChangeListener} to {@link IZkChildListener}.
 */
public class ChildListenerAdapter implements RecursivePersistListener {
  private final ChildChangeListener _listener;

  public ChildListenerAdapter(ChildChangeListener listener) {
    _listener = listener;
  }

  private static ChildChangeListener.ChangeType convertType(Watcher.Event.EventType eventType) {
    switch (eventType) {
      case NodeCreated: return ChildChangeListener.ChangeType.ENTRY_CREATED;
      case NodeDataChanged: return ChildChangeListener.ChangeType.ENTRY_DATA_CHANGE;
      case NodeDeleted: return ChildChangeListener.ChangeType.ENTRY_DELETED;
      default: throw new IllegalArgumentException("EventType " + eventType + " is not supported.");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChildListenerAdapter that = (ChildListenerAdapter) o;
    return _listener.equals(that._listener);
  }

  @Override
  public int hashCode() {
    return _listener.hashCode();
  }

  @Override
  public void handleZNodeChange(String dataPath, Watcher.Event.EventType eventType)
      throws Exception {
    _listener.handleChildChange(dataPath, convertType(eventType));
  }
}
