package org.apache.helix.integration;

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

import org.apache.helix.ExternalCommand;
import org.apache.helix.ScriptTestHelper;
import org.apache.helix.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestExternalCmd {

  @Test
  public void testExternalCmd() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    ExternalCommand cmd = ScriptTestHelper.runCommandLineTest("dummy.sh");
    String output = cmd.getStringOutput("UTF8");
    int idx = output.indexOf("this is a dummy test for verify ExternalCommand works");
    Assert.assertNotSame(idx, -1);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));

  }
}
