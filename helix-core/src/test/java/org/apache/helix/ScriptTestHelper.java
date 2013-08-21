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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.testng.Assert;

public class ScriptTestHelper {
  private static final Logger LOG = Logger.getLogger(ScriptTestHelper.class);

  public static final String INTEGRATION_SCRIPT_DIR = "src/main/scripts/integration-test/script";
  public static final String INTEGRATION_TEST_DIR = "src/main/scripts/integration-test/testcases";
  public static final String INTEGRATION_LOG_DIR = "src/main/scripts/integration-test/var/log";

  public static final long EXEC_TIMEOUT = 1200;

  public static String getPrefix() {
    StringBuilder prefixBuilder = new StringBuilder("");
    String prefix = "";
    String filepath = INTEGRATION_SCRIPT_DIR;
    File integrationScriptDir = new File(filepath);

    while (!integrationScriptDir.exists()) {
      prefixBuilder.append("../");
      prefix = prefixBuilder.toString();

      integrationScriptDir = new File(prefix + filepath);

      // Give up
      if (prefix.length() > 30) {
        return "";
      }
    }
    return new File(prefix).getAbsolutePath() + "/";
  }

  public static ExternalCommand runCommandLineTest(String testName, String... arguments)
      throws IOException, InterruptedException, TimeoutException {
    ExternalCommand cmd =
        ExternalCommand.executeWithTimeout(new File(getPrefix() + INTEGRATION_TEST_DIR), testName,
            EXEC_TIMEOUT, arguments);
    int exitValue = cmd.exitValue();
    String output = cmd.getStringOutput("UTF8");

    if (0 == exitValue) {
      LOG.info("Test " + testName + " has run. ExitCode=" + exitValue + ". Command output: "
          + output);
    } else {
      LOG.warn("Test " + testName + " is FAILING. ExitCode=" + exitValue + ". Command output: "
          + output);
      Assert.fail(output);
      // return cmd;
    }
    return cmd;
  }
}
