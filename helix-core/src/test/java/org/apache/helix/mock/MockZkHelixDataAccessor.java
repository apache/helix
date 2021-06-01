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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;

public class MockZkHelixDataAccessor extends ZKHelixDataAccessor {
  Map<PropertyType, Integer> _readPathCounters = new HashMap<>();

  public MockZkHelixDataAccessor(String clusterName, BaseDataAccessor<ZNRecord> baseDataAccessor) {
    super(clusterName, baseDataAccessor);
  }

  @Deprecated
  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys) {
    return getProperty(keys, false);
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys, boolean throwException) {
    for (PropertyKey key : keys) {
      addCount(key);
    }
    return super.getProperty(keys, throwException);
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    addCount(key);
    return super.getProperty(key);
  }

  @Deprecated
  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key) {
    return getChildValuesMap(key, false);
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key, boolean throwException) {
    Map<String, T> map = super.getChildValuesMap(key, throwException);
    addCount(key, map.keySet().size());
    return map;
  }

  private void addCount(PropertyKey key) {
    addCount(key, 1);
  }

  private void addCount(PropertyKey key, int count) {
    PropertyType type = key.getType();
    if (!_readPathCounters.containsKey(type)) {
      _readPathCounters.put(type, 0);
    }
    _readPathCounters.put(type, _readPathCounters.get(type) + count);
  }

  public int getReadCount(PropertyType type) {
    if (_readPathCounters.containsKey(type)) {
      return _readPathCounters.get(type);
    }

    return 0;
  }

  public void clearReadCounters() {
    _readPathCounters.clear();
  }
}
