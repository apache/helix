package org.apache.helix.model;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestIdealState
{
  @Test
  public void testGetInstanceSet()
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;
    System.out.println("START " + testName + " at "
        + new Date(System.currentTimeMillis()));


    IdealState idealState = new IdealState("idealState");
    idealState.getRecord().setListField("TestDB_0", Arrays.asList("node_1", "node_2"));
    Map<String, String> instanceState = new HashMap<String, String>();
    instanceState.put("node_3", "MASTER");
    instanceState.put("node_4", "SLAVE");
    idealState.getRecord().setMapField("TestDB_1", instanceState);

    // test AUTO mode
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
    Set<String> instances = idealState.getInstanceSet("TestDB_0");
//    System.out.println("instances: " + instances);
    Assert.assertEquals(instances.size(), 2, "Should contain node_1 and node_2");
    Assert.assertTrue(instances.contains("node_1"), "Should contain node_1 and node_2");
    Assert.assertTrue(instances.contains("node_2"), "Should contain node_1 and node_2");

    instances = idealState.getInstanceSet("TestDB_nonExist_auto");
    Assert.assertEquals(instances, Collections.emptySet(), "Should get empty set");
    
    // test CUSTOMIZED mode
    idealState.setIdealStateMode(IdealStateModeProperty.CUSTOMIZED.toString());
    instances = idealState.getInstanceSet("TestDB_1");
//    System.out.println("instances: " + instances);
    Assert.assertEquals(instances.size(), 2, "Should contain node_3 and node_4");
    Assert.assertTrue(instances.contains("node_3"), "Should contain node_3 and node_4");
    Assert.assertTrue(instances.contains("node_4"), "Should contain node_3 and node_4");

    instances = idealState.getInstanceSet("TestDB_nonExist_custom");
    Assert.assertEquals(instances, Collections.emptySet(), "Should get empty set");
    
    System.out.println("END " + testName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
