package org.apache.helix.webapp;

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
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.webapp.resources.JsonParameters;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisableResource extends AdminTestBase {

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    String instanceUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/resourceGroups/"
            + "TestDB0";

    // Disable TestDB0
    Map<String, String> paramMap = new HashMap<String, String>();
    paramMap.put(JsonParameters.MANAGEMENT_COMMAND, ClusterSetup.enableResource);
    paramMap.put(JsonParameters.ENABLED, Boolean.toString(false));
    TestHelixAdminScenariosRest.assertSuccessPostOperation(instanceUrl, paramMap, false);

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    Assert.assertFalse(idealState.isEnabled());

    // Re-enable TestDB0
    paramMap.put(JsonParameters.ENABLED, Boolean.toString(true));
    TestHelixAdminScenariosRest.assertSuccessPostOperation(instanceUrl, paramMap, false);

    idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    Assert.assertTrue(idealState.isEnabled());

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
