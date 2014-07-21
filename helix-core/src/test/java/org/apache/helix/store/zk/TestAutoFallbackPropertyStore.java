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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAutoFallbackPropertyStore extends ZkTestBase {

  class MyDataUpdater implements DataUpdater<ZNRecord> {
    final String _id;

    public MyDataUpdater(String id) {
      _id = id;
    }

    @Override
    public ZNRecord update(ZNRecord currentData) {
      if (currentData == null) {
        currentData = new ZNRecord(_id);
      } else {
        currentData.setSimpleField("key", "value");
      }
      return currentData;
    }
  }

  @Test
  public void testSingleUpdateUsingFallbackPath() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0 under fallbackRoot
    for (int i = 0; i < 1; i++) {
      String path = String.format("%s/%d", fallbackRoot, i);
      baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    String path = String.format("/%d", 0);
    Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
        "Should not exist under new location");
    Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
        "Should exist under fallback location");

    boolean succeed = store.update(path, new MyDataUpdater("new0"), AccessOption.PERSISTENT);
    Assert.assertTrue(succeed);

    // fallback path should remain unchanged
    ZNRecord record = baseAccessor.get(String.format("%s%s", fallbackRoot, path), null, 0);
    Assert.assertNotNull(record);
    Assert.assertEquals(record.getId(), "0");
    Assert.assertNull(record.getSimpleField("key"));

    // new path should have simple field set
    record = baseAccessor.get(String.format("%s%s", root, path), null, 0);
    Assert.assertNotNull(record);
    Assert.assertEquals(record.getId(), "0");
    Assert.assertNotNull(record.getSimpleField("key"));
    Assert.assertEquals(record.getSimpleField("key"), "value");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSingleUpdateUsingNewPath() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0 under both fallbackRoot and root
    for (int i = 0; i < 1; i++) {
      String path = String.format("%s/%d", root, i);
      baseAccessor.create(path, new ZNRecord("new" + i), AccessOption.PERSISTENT);

      path = String.format("%s/%d", fallbackRoot, i);
      baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    String path = String.format("/%d", 0);
    Assert.assertTrue(baseAccessor.exists(String.format("%s%s", root, path), 0),
        "Should exist under new location");
    Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
        "Should exist under fallback location");

    boolean succeed = store.update(path, new MyDataUpdater("0"), AccessOption.PERSISTENT);
    Assert.assertTrue(succeed);

    ZNRecord record = baseAccessor.get(String.format("%s%s", fallbackRoot, path), null, 0);
    Assert.assertNotNull(record);
    Assert.assertEquals(record.getId(), "0");
    Assert.assertNull(record.getSimpleField("key"));

    record = baseAccessor.get(String.format("%s%s", root, path), null, 0);
    Assert.assertNotNull(record);
    Assert.assertEquals(record.getId(), "new0");
    Assert.assertNotNull(record.getSimpleField("key"));
    Assert.assertEquals(record.getSimpleField("key"), "value");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMultiUpdateUsingFallbackPath() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0-9 under fallbackRoot
    for (int i = 0; i < 10; i++) {
      String path = String.format("%s/%d", fallbackRoot, i);
      baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    List<String> paths = new ArrayList<String>();
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
    for (int i = 0; i < 10; i++) {
      String path = String.format("/%d", i);
      Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
          "Should not exist under new location");
      Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
          "Should exist under fallback location");
      paths.add(path);
      updaters.add(new MyDataUpdater("new" + i));
    }

    boolean succeed[] = store.updateChildren(paths, updaters, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(succeed[i]);
      String path = paths.get(i);

      // fallback path should remain unchanged
      ZNRecord record = baseAccessor.get(String.format("%s%s", fallbackRoot, path), null, 0);
      Assert.assertNotNull(record);
      Assert.assertEquals(record.getId(), "" + i);
      Assert.assertNull(record.getSimpleField("key"));

      // new path should have simple field set
      record = baseAccessor.get(String.format("%s%s", root, path), null, 0);
      Assert.assertNotNull(record);
      Assert.assertEquals(record.getId(), "" + i);
      Assert.assertNotNull(record.getSimpleField("key"));
      Assert.assertEquals(record.getSimpleField("key"), "value");
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMultiUpdateUsingNewath() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0-9 under both fallbackRoot and new root
    for (int i = 0; i < 10; i++) {
      String path = String.format("%s/%d", fallbackRoot, i);
      baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);

      path = String.format("%s/%d", root, i);
      baseAccessor.create(path, new ZNRecord("new" + i), AccessOption.PERSISTENT);
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    List<String> paths = new ArrayList<String>();
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
    for (int i = 0; i < 20; i++) {
      String path = String.format("/%d", i);
      if (i < 10) {
        Assert.assertTrue(baseAccessor.exists(String.format("%s%s", root, path), 0),
            "Should exist under new location");
        Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
            "Should exist under fallback location");
      } else {
        Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
            "Should not exist under new location");
        Assert.assertFalse(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
            "Should not exist under fallback location");
      }
      paths.add(path);
      updaters.add(new MyDataUpdater("new" + i));
    }

    boolean succeed[] = store.updateChildren(paths, updaters, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(succeed[i]);
      String path = paths.get(i);

      // fallback path should remain unchanged
      if (i < 10) {
        ZNRecord record = baseAccessor.get(String.format("%s%s", fallbackRoot, path), null, 0);
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getId(), "" + i);
        Assert.assertNull(record.getSimpleField("key"));
      } else {
        Assert.assertFalse(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
            "Should not exist under fallback location");
      }

      // new path should have simple field set
      ZNRecord record = baseAccessor.get(String.format("%s%s", root, path), null, 0);
      Assert.assertNotNull(record);
      Assert.assertEquals(record.getId(), "new" + i);
      if (i < 10) {
        Assert.assertNotNull(record.getSimpleField("key"));
        Assert.assertEquals(record.getSimpleField("key"), "value");
      } else {
        Assert.assertNull(record.getSimpleField("key"));
      }

    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSingleSet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0 under fallbackRoot
    for (int i = 0; i < 1; i++) {
      String path = String.format("%s/%d", fallbackRoot, i);
      baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    String path = String.format("/%d", 0);
    Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
        "Should not exist under new location");
    Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
        "Should exist under fallback location");
    ZNRecord record = new ZNRecord("new0");
    boolean succeed = store.set(path, record, AccessOption.PERSISTENT);
    Assert.assertTrue(succeed);

    record = baseAccessor.get(String.format("%s%s", fallbackRoot, path), null, 0);
    Assert.assertNotNull(record);
    Assert.assertEquals(record.getId(), "0");

    record = baseAccessor.get(String.format("%s%s", root, path), null, 0);
    Assert.assertNotNull(record);
    Assert.assertEquals(record.getId(), "new0");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMultiSet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0-9 under fallbackRoot
    for (int i = 0; i < 10; i++) {
      String path = String.format("%s/%d", fallbackRoot, i);
      baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    List<String> paths = new ArrayList<String>();
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (int i = 0; i < 10; i++) {
      String path = String.format("/%d", i);
      Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
          "Should not exist under new location");
      Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
          "Should exist under fallback location");
      paths.add(path);
      ZNRecord record = new ZNRecord("new" + i);
      records.add(record);
    }

    boolean succeed[] = store.setChildren(paths, records, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(succeed[i]);
      String path = String.format("/%d", i);
      ZNRecord record = baseAccessor.get(String.format("%s%s", fallbackRoot, path), null, 0);
      Assert.assertNotNull(record);
      Assert.assertEquals(record.getId(), Integer.toString(i));

      record = baseAccessor.get(String.format("%s%s", root, path), null, 0);
      Assert.assertNotNull(record);
      Assert.assertEquals(record.getId(), "new" + i);
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSingleGetOnFallbackPath() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0 under fallbackRoot
    for (int i = 0; i < 1; i++) {
      String path = String.format("%s/%d", fallbackRoot, i);
      baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    String path = String.format("/%d", 0);
    Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
        "Should not exist under new location");
    Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
        "Should exist under fallback location");

    // test single exist
    boolean exist = store.exists(path, 0);
    Assert.assertTrue(exist);

    // test single getStat
    Stat stat = store.getStat(path, 0);
    Assert.assertNotNull(stat);

    // test single get
    ZNRecord record = store.get(path, null, 0);
    Assert.assertNotNull(record);
    Assert.assertEquals(record.getId(), "0");
    Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
        "Should not exist under new location after get");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  void testMultiGetOnFallbackPath() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0-9 under fallbackRoot
    for (int i = 0; i < 10; i++) {
      String path = String.format("%s/%d", fallbackRoot, i);
      baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    List<String> paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      String path = String.format("/%d", i);
      Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
          "Should not exist under new location");
      Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
          "Should exist under fallback location");
      paths.add(path);
    }

    // test multi-exist
    boolean exists[] = store.exists(paths, 0);
    for (int i = 0; i < paths.size(); i++) {
      Assert.assertTrue(exists[i]);
    }

    // test multi-getStat
    Stat stats[] = store.getStats(paths, 0);
    for (int i = 0; i < paths.size(); i++) {
      Assert.assertNotNull(stats[i]);
    }

    // test multi-get
    List<ZNRecord> records = store.get(paths, null, 0);
    Assert.assertNotNull(records);
    Assert.assertEquals(records.size(), 10);
    for (int i = 0; i < 10; i++) {
      ZNRecord record = records.get(i);
      String path = paths.get(i);
      Assert.assertNotNull(record);
      Assert.assertEquals(record.getId(), Integer.toString(i));
      Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
          "Should not exist under new location after get");
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testFailOnSingleGet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    String path = String.format("/%d", 0);
    Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
        "Should not exist under new location");
    Assert.assertFalse(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
        "Should not exist under fallback location");

    // test single exist
    boolean exist = store.exists(path, 0);
    Assert.assertFalse(exist);

    // test single getStat
    Stat stat = store.getStat(path, 0);
    Assert.assertNull(stat);

    // test single get
    ZNRecord record = store.get(path, null, 0);
    Assert.assertNull(record);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testFailOnMultiGet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0-9 under fallbackRoot
    for (int i = 0; i < 10; i++) {
      String path = String.format("%s/%d", fallbackRoot, i);
      baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    List<String> paths = new ArrayList<String>();
    for (int i = 0; i < 20; i++) {
      String path = String.format("/%d", i);
      Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
          "Should not exist under new location");
      if (i < 10) {
        Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
            "Should exist under fallback location");
      } else {
        Assert.assertFalse(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
            "Should not exist under fallback location");
      }
      paths.add(path);
    }

    // test multi-exist
    boolean exists[] = store.exists(paths, 0);
    for (int i = 0; i < paths.size(); i++) {
      if (i < 10) {
        Assert.assertTrue(exists[i]);
      } else {
        Assert.assertFalse(exists[i]);
      }
    }

    // test multi-getStat
    Stat stats[] = store.getStats(paths, 0);
    for (int i = 0; i < paths.size(); i++) {
      if (i < 10) {
        Assert.assertNotNull(stats[i]);
      } else {
        Assert.assertNull(stats[i]);
      }
    }

    // test multi-get
    List<ZNRecord> records = store.get(paths, null, 0);
    Assert.assertNotNull(records);
    Assert.assertEquals(records.size(), 20);
    for (int i = 0; i < 20; i++) {
      ZNRecord record = records.get(i);
      String path = paths.get(i);
      if (i < 10) {
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getId(), Integer.toString(i));
      } else {
        Assert.assertNull(record);
      }
      Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
          "Should not exist under new location after get");
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testGetChildren() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String root = String.format("/%s/%s", clusterName, PropertyType.PROPERTYSTORE.name());
    String fallbackRoot = String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE");
    ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    // create 0-9 under fallbackRoot and 10-19 under root
    for (int i = 0; i < 20; i++) {

      if (i < 10) {
        String path = String.format("%s/%d", fallbackRoot, i);
        baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
      } else {
        String path = String.format("%s/%d", root, i);
        baseAccessor.create(path, new ZNRecord(Integer.toString(i)), AccessOption.PERSISTENT);
      }
    }

    AutoFallbackPropertyStore<ZNRecord> store =
        new AutoFallbackPropertyStore<ZNRecord>(baseAccessor, root, fallbackRoot);

    List<String> paths = new ArrayList<String>();
    for (int i = 0; i < 20; i++) {
      String path = String.format("/%d", i);
      if (i < 10) {
        Assert.assertTrue(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
            "Should exist under fallback location");
        Assert.assertFalse(baseAccessor.exists(String.format("%s%s", root, path), 0),
            "Should not exist under new location");

      } else {
        Assert.assertFalse(baseAccessor.exists(String.format("%s%s", fallbackRoot, path), 0),
            "Should not exist under fallback location");
        Assert.assertTrue(baseAccessor.exists(String.format("%s%s", root, path), 0),
            "Should exist under new location");

      }
      paths.add(path);
    }

    List<String> childs = store.getChildNames("/", 0);
    Assert.assertNotNull(childs);
    Assert.assertEquals(childs.size(), 20);
    for (int i = 0; i < 20; i++) {
      String child = childs.get(i);
      Assert.assertTrue(childs.contains(child));
    }

    List<ZNRecord> records = store.getChildren("/", null, 0);
    Assert.assertNotNull(records);
    Assert.assertEquals(records.size(), 20);
    for (int i = 0; i < 20; i++) {
      ZNRecord record = records.get(i);
      Assert.assertNotNull(record);
      String id = record.getId();
      Assert.assertTrue(childs.contains(id));
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
