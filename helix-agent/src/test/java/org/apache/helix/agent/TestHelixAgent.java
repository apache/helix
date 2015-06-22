package org.apache.helix.agent;

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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.ExternalCommand;
import org.apache.helix.ScriptTestHelper;
import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestHelixAgent extends ZkTestBase {
  private final static Logger LOG = Logger.getLogger(TestHelixAgent.class);

  final String workingDir = ScriptTestHelper.getPrefix() + ScriptTestHelper.INTEGRATION_SCRIPT_DIR;
  ExternalCommand serverCmd = null;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    serverCmd = ExternalCommand.start(workingDir + "/simpleHttpServer.py");
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    if (serverCmd != null) {
      // shutdown server
      ExternalCommand.execute(new File(workingDir), "simpleHttpClient.py", "exit");
      // System.out.println("simpleHttpServer output: \n" + serverCmd.getStringOutput());

      // check server has received all the requests
      String serverOutput = serverCmd.getStringOutput();
      int idx = serverOutput.indexOf("requestPath: /OFFLINE-SLAVE");
      Assert.assertTrue(idx > 0, "server should receive OFFINE->SLAVE transition");

      idx = serverOutput.indexOf("requestPath: /SLAVE-MASTER", idx);
      Assert.assertTrue(idx > 0, "server should receive SLAVE-MASTER transition");

      idx = serverOutput.indexOf("requestPath: /MASTER-SLAVE", idx);
      Assert.assertTrue(idx > 0, "server should receive MASTER-SLAVE transition");

      idx = serverOutput.indexOf("requestPath: /SLAVE-OFFLINE", idx);
      Assert.assertTrue(idx > 0, "server should receive SLAVE-OFFLINE transition");

    }
  }

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;
    final int n = 1;
    final String zkAddr = _zkaddr;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, zkAddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        1, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    // set cluster config
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    ConfigAccessor configAccessor = new ConfigAccessor(_zkclient);

    // String pidFile = ScriptTestHelper.getPrefix() + ScriptTestHelper.INTEGRATION_LOG_DIR +
    // "/default/foo_{PARTITION_NAME}_pid.txt";

    // the pid file path for the first partition
    // delete it if exists
    // String pidFileFirstPartition = ScriptTestHelper.getPrefix() +
    // ScriptTestHelper.INTEGRATION_LOG_DIR + "/default/foo_TestDB0_0_pid.txt";
    // File file = new File(pidFileFirstPartition);
    // if (file.exists()) {
    // file.delete();
    // }

    // set commands for state-transitions
    CommandConfig.Builder builder = new CommandConfig.Builder();
    CommandConfig cmdConfig =
        builder.setTransition("SLAVE", "MASTER").setCommand("simpleHttpClient.py SLAVE-MASTER")
            .setCommandWorkingDir(workingDir).setCommandTimeout("0")
            // .setPidFile(pidFile)
            .build();
    configAccessor.set(scope, cmdConfig.toKeyValueMap());

    builder = new CommandConfig.Builder();
    cmdConfig =
        builder.setTransition("OFFLINE", "SLAVE").setCommand("simpleHttpClient.py OFFLINE-SLAVE")
            .setCommandWorkingDir(workingDir).build();
    configAccessor.set(scope, cmdConfig.toKeyValueMap());

    builder = new CommandConfig.Builder();
    cmdConfig =
        builder.setTransition("MASTER", "SLAVE").setCommand("simpleHttpClient.py MASTER-SLAVE")
            .setCommandWorkingDir(workingDir).build();
    configAccessor.set(scope, cmdConfig.toKeyValueMap());

    builder = new CommandConfig.Builder();
    cmdConfig =
        builder.setTransition("SLAVE", "OFFLINE").setCommand("simpleHttpClient.py SLAVE-OFFLINE")
            .setCommandWorkingDir(workingDir).build();
    configAccessor.set(scope, cmdConfig.toKeyValueMap());

    builder = new CommandConfig.Builder();
    cmdConfig =
        builder.setTransition("OFFLINE", "DROPPED").setCommand(CommandAttribute.NOP.getName())
            .build();
    configAccessor.set(scope, cmdConfig.toKeyValueMap());

    // start controller
    MockController controller = new MockController(zkAddr, clusterName, "controller_0");
    controller.syncStart();

    // start helix-agent
    Map<String, Thread> agents = new HashMap<String, Thread>();
    for (int i = 0; i < n; i++) {
      final String instanceName = "localhost_" + (12918 + i);
      Thread agentThread = new Thread() {
        @Override
        public void run() {
          try {
            HelixAgentMain.main(new String[] {
                "--zkSvr", zkAddr, "--cluster", clusterName, "--instanceName", instanceName,
                "--stateModel", "MasterSlave"
            });
          } catch (Exception e) {
            LOG.error("Exception start helix-agent", e);
          }
        }
      };
      agents.put(instanceName, agentThread);
      agentThread.start();

      // wait participant thread to start
      Thread.sleep(1000);
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // read the pid file should get current process id
    // String readPid = SystemUtil.getPidFromFile(new File(pidFileFirstPartition));
    // Assert.assertNotNull(readPid, "readPid is the pid for foo_test.py. should NOT be null");

    // String name = ManagementFactory.getRuntimeMXBean().getName();
    // String currentPid = name.substring(0,name.indexOf("@"));

    // System.out.println("read-pid: " + readPid + ", current-pid: " + currentPid);

    // drop resource will trigger M->S and S->O transitions
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", _zkaddr, "--dropResource", clusterName, "TestDB0"
    });
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (Thread agentThread : agents.values()) {
      agentThread.interrupt();
    }
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
