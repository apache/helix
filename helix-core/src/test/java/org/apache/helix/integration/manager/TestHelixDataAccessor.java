package org.apache.helix.integration.manager;

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
import org.apache.helix.controller.ResourceControllerDataProvider;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.MockZkClient;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestHelixDataAccessor extends ZkTestBase {
  private MockZkClient _zkClient;
  BaseDataAccessor<ZNRecord> baseDataAccessor;
  HelixDataAccessor accessor;
  List<PropertyKey> propertyKeys;

  @BeforeClass
  public void beforeClass() {
    _zkClient = new MockZkClient(ZK_ADDR);

    baseDataAccessor = new ZkBaseDataAccessor<>(_zkClient);
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

  @Test
  public void testHelixDataAccessorReadData() {
    accessor.getProperty(new ArrayList<>(propertyKeys), false);
    try {
      accessor.getProperty(new ArrayList<>(propertyKeys), true);
      Assert.fail();
    } catch (HelixMetaDataAccessException ex) {
    }

    PropertyKey idealStates = accessor.keyBuilder().idealStates();
    accessor.getChildValues(idealStates, false);
    try {
      accessor.getChildValues(idealStates, true);
      Assert.fail();
    } catch (HelixMetaDataAccessException ex) {
    }

    accessor.getChildValuesMap(idealStates, false);
    try {
      accessor.getChildValuesMap(idealStates, true);
      Assert.fail();
    } catch (HelixMetaDataAccessException ex) {
    }
  }

  @Test (expectedExceptions = {HelixMetaDataAccessException.class})
  public void testDataProviderRefresh() {
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider("MyCluster");
    cache.refresh(accessor);
  }
}
