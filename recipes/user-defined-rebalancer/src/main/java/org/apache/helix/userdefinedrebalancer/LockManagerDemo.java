package org.apache.helix.userdefinedrebalancer;

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
import java.io.InputStream;
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
import org.apache.helix.tools.YAMLClusterSetup;
import org.apache.log4j.Logger;

public class LockManagerDemo {
  private static final Logger LOG = Logger.getLogger(LockManagerDemo.class);

  /**
   * LockManagerDemo clusterName, numInstances, lockGroupName, numLocks
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    final String zkAddress = "localhost:2199";

    // default participant parameters in case the config does not specify them
    int numInstances = 3;
    boolean instancesSpecified = false;
    Thread[] processArray = new Thread[numInstances];

    // HelixManager for setting up the controller
    HelixManager controllerManager = null;

    // Name of the lock group resource (specified by the config file)
    String lockGroupName = null;
    try {
      startLocalZookeeper(2199);
      YAMLClusterSetup setup = new YAMLClusterSetup(zkAddress);
      InputStream input =
          Thread.currentThread().getContextClassLoader()
              .getResourceAsStream("lock-manager-config.yaml");
      final YAMLClusterSetup.YAMLClusterConfig config = setup.setupCluster(input);
      if (config == null) {
        LOG.error("Invalid YAML configuration");
        return;
      }
      if (config.resources == null || config.resources.isEmpty()) {
        LOG.error("Need to specify a resource!");
        return;
      }

      // save resource name
      lockGroupName = config.resources.get(0).name;

      // save participants if specified
      if (config.participants != null && config.participants.size() > 0) {
        numInstances = config.participants.size();
        instancesSpecified = true;
        processArray = new Thread[numInstances];
      }

      // run each participant
      for (int i = 0; i < numInstances; i++) {
        String participantName;
        if (instancesSpecified) {
          participantName = config.participants.get(i).name;
        } else {
          participantName = "localhost_" + (12000 + i);
        }
        final String instanceName = participantName;
        processArray[i] = new Thread(new Runnable() {

          @Override
          public void run() {
            LockProcess lockProcess = null;

            try {
              lockProcess =
                  new LockProcess(config.clusterName, zkAddress, instanceName,
                      config.resources.get(0).stateModel.name);
              lockProcess.start();
              Thread.currentThread().join();
            } catch (InterruptedException e) {
              System.out.println(instanceName + " Interrupted");
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

      // start the controller
      controllerManager =
          HelixControllerMain.startHelixController(zkAddress, config.clusterName, "controller",
              HelixControllerMain.STANDALONE);
      Thread.sleep(5000);

      // HelixAdmin for querying cluster state
      HelixAdmin admin = new ZKHelixAdmin(zkAddress);

      printStatus(admin, config.clusterName, lockGroupName);

      // stop one participant
      System.out.println("Stopping the first participant");
      processArray[0].interrupt();
      Thread.sleep(3000);
      printStatus(admin, config.clusterName, lockGroupName);
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
    TreeSet<String> treeSet = new TreeSet<String>(externalView.getPartitionSet());
    System.out.println("lockName" + "\t" + "acquired By");
    System.out.println("======================================");
    for (String lockName : treeSet) {
      Map<String, String> stateMap = externalView.getStateMap(lockName);
      String acquiredBy = null;
      if (stateMap != null) {
        for (String instanceName : stateMap.keySet()) {
          if ("LOCKED".equals(stateMap.get(instanceName))) {
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
    server = new ZkServer(dataDir, logDir, defaultNameSpace, zkPort);
    server.start();
  }

}
