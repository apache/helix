package org.apache.helix.integration.manager;

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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.MockZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestHelixDataAccessor extends ZkTestBase {
  private MockZkClient _zkClient;
  private HelixDataAccessor accessor;
  private List<PropertyKey> propertyKeys;

  @BeforeClass
  public void beforeClass() {
    _zkClient = new MockZkClient(ZK_ADDR);

    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(_zkClient);
    accessor = new ZKHelixDataAccessor("HELIX", baseDataAccessor);

    Map<String, HelixProperty> paths = new TreeMap<>();
    propertyKeys = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      PropertyKey key = accessor.keyBuilder().idealStates("RESOURCE" + i);
      propertyKeys.add(key);
      paths.put(key.getPath(), new HelixProperty("RESOURCE" + i));
      accessor.setProperty(key, paths.get(key.getPath()));
    }

    List<HelixProperty> data = accessor.getProperty(new ArrayList<>(propertyKeys), true);
    Assert.assertEquals(data.size(), 5);

    PropertyKey key = accessor.keyBuilder().idealStates("RESOURCE6");
    propertyKeys.add(key);
    _zkClient.putData(key.getPath(), null);
  }

  @AfterClass
  public void afterClass() {
    _zkClient.deleteRecursively("/HELIX");
  }

  @Test
  public void testHelixDataAccessorReadData() {
    accessor.getProperty(new ArrayList<>(propertyKeys), false);
    try {
      accessor.getProperty(new ArrayList<>(propertyKeys), true);
      Assert.fail();
    } catch (HelixMetaDataAccessException ignored) {
    }

    PropertyKey idealStates = accessor.keyBuilder().idealStates();
    accessor.getChildValues(idealStates, false);
    try {
      accessor.getChildValues(idealStates, true);
      Assert.fail();
    } catch (HelixMetaDataAccessException ignored) {
    }

    accessor.getChildValuesMap(idealStates, false);
    try {
      accessor.getChildValuesMap(idealStates, true);
      Assert.fail();
    } catch (HelixMetaDataAccessException ignored) {
    }
  }

  @Test(expectedExceptions = {
      HelixMetaDataAccessException.class
  })
  public void testDataProviderRefresh() {
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider("MyCluster");
    cache.refresh(accessor);
  }
}
