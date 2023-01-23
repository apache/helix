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

import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.zookeeper.Watcher;


/**
 * A Adapter class to transform {@link DataChangeListener} to {@link IZkDataListener}
 */
public class DataListenerAdapter implements IZkDataListener {
  private final DataChangeListener _listener;

  public DataListenerAdapter(DataChangeListener listener) {
    _listener = listener;
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {
    throw new UnsupportedOperationException("handleDataChange(String dataPath, Object data) is not supported.");
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
    handleDataChange(dataPath, null, Watcher.Event.EventType.NodeDeleted);
  }

  @Override
  public void handleDataChange(String dataPath, Object data, Watcher.Event.EventType eventType) throws Exception {
    _listener.handleDataChange(dataPath, data, convertType(eventType));
  }

  private static DataChangeListener.ChangeType convertType(Watcher.Event.EventType eventType) {
    switch (eventType) {
      case NodeCreated: return DataChangeListener.ChangeType.ENTRY_CREATED;
      case NodeDataChanged: return DataChangeListener.ChangeType.ENTRY_UPDATE;
      case NodeDeleted: return DataChangeListener.ChangeType.ENTRY_DELETED;
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
    DataListenerAdapter that = (DataListenerAdapter) o;
    return _listener.equals(that._listener);
  }

  @Override
  public int hashCode() {
    return _listener.hashCode();
  }
}
