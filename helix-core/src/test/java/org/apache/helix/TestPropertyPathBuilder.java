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

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test
public class TestPropertyPathBuilder {
  @Test
  public void testGetPath() {
    String actual;
    actual = PropertyPathBuilder.idealState("test_cluster");
    AssertJUnit.assertEquals(actual, "/test_cluster/IDEALSTATES");
    actual = PropertyPathBuilder.idealState("test_cluster", "resource");
    AssertJUnit.assertEquals(actual, "/test_cluster/IDEALSTATES/resource");

    actual = PropertyPathBuilder.instance("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1");

    actual = PropertyPathBuilder.instanceCurrentState("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CURRENTSTATES");
    actual = PropertyPathBuilder.instanceCurrentState("test_cluster", "instanceName1", "sessionId");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CURRENTSTATES/sessionId");

    actual = PropertyPathBuilder.controller("test_cluster");
    AssertJUnit.assertEquals(actual, "/test_cluster/CONTROLLER");
    actual = PropertyPathBuilder.controllerMessage("test_cluster");
    AssertJUnit.assertEquals(actual, "/test_cluster/CONTROLLER/MESSAGES");

  }
}
