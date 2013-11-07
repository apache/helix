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

public class TestFailOverPerf1kp {
  // TODO: renable this test. disable it because the script is not running properly on apache
  // jenkins
  // @Test
  public void testFailOverPerf1kp() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    ExternalCommand cmd = ScriptTestHelper.runCommandLineTest("helix_random_kill_local_startzk.sh");
    String output = cmd.getStringOutput("UTF8");
    int i = getStateTransitionLatency(0, output);
    int j = output.indexOf("ms", i);
    long latency = Long.parseLong(output.substring(i, j));
    System.out.println("startup latency: " + latency);

    i = getStateTransitionLatency(i, output);
    j = output.indexOf("ms", i);
    latency = Long.parseLong(output.substring(i, j));
    System.out.println("failover latency: " + latency);
    Assert.assertTrue(latency < 800, "failover latency for 1k partition test should < 800ms");

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));

  }

  int getStateTransitionLatency(int start, String output) {
    final String pattern = "state transition latency: ";
    int i = output.indexOf(pattern, start) + pattern.length();
    // String latencyStr = output.substring(i, j);
    // System.out.println(latencyStr);
    return i;
  }
}
