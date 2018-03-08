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

import org.apache.helix.controller.HierarchicalDataHolder;
import org.apache.helix.manager.zk.ZkClient;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.io.FileFilter;

public class TestHierarchicalDataStore extends ZkUnitTestBase {
  protected static ZkClient _zkClientString = null;

  @Test(groups = {
    "unitTest"
  })
  public void testHierarchicalDataStore() {
    _zkClientString = new ZkClient(ZK_ADDR, 1000, 3000);

    String path = "/tmp/testHierarchicalDataStore";
    FileFilter filter = null;
    // _zkClient.setZkSerializer(new ZNRecordSerializer());

    _zkClientString.deleteRecursively(path);
    HierarchicalDataHolder<ZNRecord> dataHolder =
        new HierarchicalDataHolder<ZNRecord>(_zkClientString, path, filter);
    dataHolder.print();
    AssertJUnit.assertFalse(dataHolder.refreshData());

    // write data
    add(path, "root data");
    AssertJUnit.assertTrue(dataHolder.refreshData());
    dataHolder.print();

    // add some children
    add(path + "/child1", "child 1 data");
    add(path + "/child2", "child 2 data");
    AssertJUnit.assertTrue(dataHolder.refreshData());
    dataHolder.print();

    // add some grandchildren
    add(path + "/child1" + "/grandchild1", "grand child 1 data");
    add(path + "/child1" + "/grandchild2", "grand child 2 data");
    AssertJUnit.assertTrue(dataHolder.refreshData());
    dataHolder.print();

    AssertJUnit.assertFalse(dataHolder.refreshData());

    set(path + "/child1", "new child 1 data");
    AssertJUnit.assertTrue(dataHolder.refreshData());
    dataHolder.print();
  }

  private void set(String path, String data) {
    _zkClientString.writeData(path, data);
  }

  private void add(String path, String data) {
    _zkClientString.createPersistent(path, true);
    _zkClientString.writeData(path, data);
  }

}
