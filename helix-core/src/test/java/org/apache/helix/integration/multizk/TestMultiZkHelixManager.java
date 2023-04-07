package org.apache.helix.integration.multizk;

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

import java.util.Date;
import org.apache.helix.*;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 *
 * This test class will not set jvm routing zk config and test Helix functionality.
 * Tests were similar to TestMultiZkHelixJavaApis but without "MSDS_SERVER_ENDPOINT_KEY"
 * in system property
 */
public class TestMultiZkHelixManager extends TestMultiZkConnectionConfig {
  private static final String _className = TestHelper.getTestClassName();

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
  }

  /**
   * Test creation of HelixManager and makes sure it connects correctly.
   */
  @Override
  @Test
  public void testZKHelixManager() throws Exception {
    String methodName = TestHelper.getTestMethodName();

    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    super.testZKHelixManager();

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Test creation of HelixManager and makes sure it connects correctly.
   */
  @Override
  @Test(dependsOnMethods = "testZKHelixManager")
  public void testZKHelixManagerCloudConfig() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    super.testZKHelixManagerCloudConfig();

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }
}
