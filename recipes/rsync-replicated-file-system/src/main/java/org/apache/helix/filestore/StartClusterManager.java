package org.apache.helix.filestore;

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

import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;

public class StartClusterManager {
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("USAGE: java StartClusterManager zookeeperAddress (e.g. localhost:2181)");
      System.exit(1);
    }

    final String clusterName = SetupCluster.DEFAULT_CLUSTER_NAME;
    final String zkAddr = args[0];

    try {
      final HelixManager manager =
          HelixControllerMain.startHelixController(zkAddr, clusterName, null,
              HelixControllerMain.STANDALONE);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Shutting down cluster manager: " + manager.getInstanceName());
          manager.disconnect();
        }
      });

      Thread.currentThread().join();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
