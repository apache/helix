package org.apache.helix.manager.zk;

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

import java.util.Date;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAddBuiltInStateModelDef extends ZkUnitTestBase {

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(clusterName);
    admin.addStateModelDef(clusterName, BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition().getId(),
                           BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
    ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, clusterName);
    controller.syncStart();

    // controller shall create all built-in state model definitions
    final BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    final PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    boolean ret = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        for (BuiltInStateModelDefinitions def : BuiltInStateModelDefinitions.values()) {
          String path = keyBuilder.stateModelDef(def.getStateModelDefinition().getId()).getPath();
          boolean exist = baseAccessor.exists(path, 0);
          if (!exist) {
            return false;
          }

          // make sure MasterSlave is not over-written
          if (def == BuiltInStateModelDefinitions.MasterSlave) {
            Stat stat = new Stat();
            baseAccessor.get(path, stat, 0);
            if (stat.getVersion() != 0) {
              return false;
            }
          }
        }
        return true;
      }
    }, 10 * 1000);
    Assert.assertTrue(ret);
    controller.syncStop();
    admin.dropCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
