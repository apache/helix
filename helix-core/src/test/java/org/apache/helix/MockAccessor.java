package org.apache.helix;

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
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.StateModelDefinition;

public class MockAccessor implements HelixDataAccessor {
  private final String _clusterName;
  Map<String, ZNRecord> data = new HashMap<String, ZNRecord>();
  private final PropertyKey.Builder _propertyKeyBuilder;

  public MockAccessor() {
    this("testCluster-" + Math.random() * 10000 % 999);
  }

  public MockAccessor(String clusterName) {
    _clusterName = clusterName;
    _propertyKeyBuilder = new PropertyKey.Builder(_clusterName);
  }

  Map<String, ZNRecord> map = new HashMap<String, ZNRecord>();

  @Override
  public boolean createStateModelDef(StateModelDefinition stateModelDef) {
    return false;
  }

  @Override
  public boolean createControllerMessage(Message message) {
    return false;
  }

  @Override
  public boolean createControllerLeader(LiveInstance leader) {
    return false;
  }

  @Override
  public boolean createPause(PauseSignal pauseSignal) {
    return false;
  }

  @Override
  public boolean setProperty(PropertyKey key, HelixProperty value) {
    String path = key.getPath();
    data.put(path, value.getRecord());
    return true;
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, T value) {
    String path = key.getPath();
    PropertyType type = key.getType();
    if (type.updateOnlyOnExists) {
      if (data.containsKey(path)) {
        if (type.mergeOnUpdate) {
          ZNRecord znRecord = new ZNRecord(data.get(path));
          znRecord.merge(value.getRecord());
          data.put(path, znRecord);
        } else {
          data.put(path, value.getRecord());
        }
      }
    } else {
      if (type.mergeOnUpdate) {
        if (data.containsKey(path)) {
          ZNRecord znRecord = new ZNRecord(data.get(path));
          znRecord.merge(value.getRecord());
          data.put(path, znRecord);
        } else {
          data.put(path, value.getRecord());
        }
      } else {
        data.put(path, value.getRecord());
      }
    }

    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    String path = key.getPath();
    return (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), data.get(path));
  }

  @Override
  public boolean removeProperty(PropertyKey key) {
    String path = key.getPath(); // PropertyPathConfig.getPath(type,
    // _clusterName, keys);
    data.remove(path);
    return true;
  }

  @Override
  public List<String> getChildNames(PropertyKey propertyKey) {
    List<String> child = new ArrayList<String>();
    String path = propertyKey.getPath();
    for (String key : data.keySet()) {
      if (key.startsWith(path)) {
        String[] keySplit = key.split("\\/");
        String[] pathSplit = path.split("\\/");
        if (keySplit.length > pathSplit.length) {
          child.add(keySplit[pathSplit.length]);
        }
      }
    }
    return child;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey propertyKey) {
    List<ZNRecord> childs = new ArrayList<ZNRecord>();
    String path = propertyKey.getPath(); // PropertyPathConfig.getPath(type,
    // _clusterName, keys);
    for (String key : data.keySet()) {
      if (key.startsWith(path)) {
        String[] keySplit = key.split("\\/");
        String[] pathSplit = path.split("\\/");
        if (keySplit.length - pathSplit.length == 1) {
          ZNRecord record = data.get(key);
          if (record != null) {
            childs.add(record);
          }
        } else {
          System.out.println("keySplit:" + Arrays.toString(keySplit));
          System.out.println("pathSplit:" + Arrays.toString(pathSplit));
        }
      }
    }
    return (List<T>) HelixProperty.convertToTypedList(propertyKey.getTypeClass(), childs);
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key) {
    List<T> list = getChildValues(key);
    return HelixProperty.convertListToMap(list);
  }

  @Override
  public <T extends HelixProperty> boolean[] createChildren(List<PropertyKey> keys,
      List<T> children) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends HelixProperty> boolean[] setChildren(List<PropertyKey> keys, List<T> children) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PropertyKey.Builder keyBuilder() {
    return _propertyKeyBuilder;
  }

  @Override
  public BaseDataAccessor getBaseDataAccessor() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends HelixProperty> boolean[] updateChildren(List<String> paths,
      List<DataUpdater<ZNRecord>> updaters, int options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys) {
    List<T> list = new ArrayList<T>();
    for (PropertyKey key : keys) {
      @SuppressWarnings("unchecked")
      T t = (T) getProperty(key);
      list.add(t);
    }
    return list;
  }
}
