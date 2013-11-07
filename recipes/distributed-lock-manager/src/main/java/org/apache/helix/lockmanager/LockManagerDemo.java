package org.apache.helix.lockmanager;

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
import java.util.Map;
import java.util.TreeSet;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;

public class LockManagerDemo {
  /**
   * LockManagerDemo clusterName, numInstances, lockGroupName, numLocks
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    final String zkAddress = "localhost:2199";
    final String clusterName = "lock-manager-demo";
    final String lockGroupName = "lock-group";
    final int numInstances = 3;
    final int numPartitions = 12;
    final boolean startController = false;
    HelixManager controllerManager = null;
    Thread[] processArray;
    processArray = new Thread[numInstances];
    try {
      startLocalZookeeper(2199);
      HelixAdmin admin = new ZKHelixAdmin(zkAddress);
      admin.addCluster(clusterName, true);
      admin.addStateModelDef(clusterName, "OnlineOffline", new StateModelDefinition(
          StateModelConfigGenerator.generateConfigForOnlineOffline()));
      admin.addResource(clusterName, lockGroupName, numPartitions, "OnlineOffline",
          RebalanceMode.FULL_AUTO.toString());
      admin.rebalance(clusterName, lockGroupName, 1);
      for (int i = 0; i < numInstances; i++) {
        final String instanceName = "localhost_" + (12000 + i);
        processArray[i] = new Thread(new Runnable() {

          @Override
          public void run() {
            LockProcess lockProcess = null;

            try {
              lockProcess = new LockProcess(clusterName, zkAddress, instanceName, startController);
              lockProcess.start();
              Thread.currentThread().join();
            } catch (InterruptedException e) {
              System.out.println(instanceName + "Interrupted");
              if (lockProcess != null) {
                lockProcess.stop();
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }

        });
        processArray[i].start();
      }
      Thread.sleep(3000);
      controllerManager =
          HelixControllerMain.startHelixController(zkAddress, clusterName, "controller",
              HelixControllerMain.STANDALONE);
      Thread.sleep(5000);
      printStatus(admin, clusterName, lockGroupName);
      System.out.println("Stopping localhost_12000");
      processArray[0].interrupt();
      Thread.sleep(3000);
      printStatus(admin, clusterName, lockGroupName);
      Thread.currentThread().join();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (controllerManager != null) {
        controllerManager.disconnect();
      }
      for (Thread process : processArray) {
        if (process != null) {
          process.interrupt();
        }
      }
    }
  }

  private static void printStatus(HelixAdmin admin, String cluster, String resource) {
    ExternalView externalView = admin.getResourceExternalView(cluster, resource);
    // System.out.println(externalView);
    TreeSet<String> treeSet = new TreeSet<String>(externalView.getPartitionSet());
    System.out.println("lockName" + "\t" + "acquired By");
    System.out.println("======================================");
    for (String lockName : treeSet) {
      Map<String, String> stateMap = externalView.getStateMap(lockName);
      String acquiredBy = null;
      if (stateMap != null) {
        for (String instanceName : stateMap.keySet()) {
          if ("ONLINE".equals(stateMap.get(instanceName))) {
            acquiredBy = instanceName;
            break;
          }
        }
      }
      System.out.println(lockName + "\t" + ((acquiredBy != null) ? acquiredBy : "NONE"));
    }
  }

  private static void startLocalZookeeper(int port) throws Exception {
    ZkServer server = null;
    String baseDir = "/tmp/IntegrationTest/";
    final String dataDir = baseDir + "zk/dataDir";
    final String logDir = baseDir + "/tmp/logDir";
    FileUtils.deleteDirectory(new File(dataDir));
    FileUtils.deleteDirectory(new File(logDir));

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient) {

      }
    };
    int zkPort = 2199;
    // final String zkAddress = "localhost:" + zkPort;

    server = new ZkServer(dataDir, logDir, defaultNameSpace, zkPort);
    server.start();

  }

}
