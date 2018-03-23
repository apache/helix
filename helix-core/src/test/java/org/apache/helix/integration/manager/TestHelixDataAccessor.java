package org.apache.helix.integration.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.MockZkClient;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestHelixDataAccessor extends ZkIntegrationTestBase{
  private MockZkClient _zkClient;

  @BeforeClass
  public void beforeClass() {
    _zkClient = new MockZkClient(ZK_ADDR);
  }

  @Test(expectedExceptions = HelixException.class)
  public void testHelixDataAccessorReadData() {
    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(_zkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor("HELIX", baseDataAccessor);

    Map<String, HelixProperty> paths = new TreeMap<>();
    List<PropertyKey> propertyKeys = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      PropertyKey key = accessor.keyBuilder().idealStates("RESOURCE" + i);
      propertyKeys.add(key);
      paths.put(key.getPath(), new HelixProperty("RESOURCE" + i));
      accessor.setProperty(key, paths.get(key.getPath()));
    }

    List<HelixProperty> data = accessor.getProperty(new ArrayList<>(propertyKeys));
    Assert.assertEquals(data.size(), 5);

    PropertyKey key = accessor.keyBuilder().idealStates("RESOURCE6");
    propertyKeys.add(key);
    _zkClient.putData(key.getPath(), null);
    accessor.getProperty(new ArrayList<>(propertyKeys), true);
  }
}
