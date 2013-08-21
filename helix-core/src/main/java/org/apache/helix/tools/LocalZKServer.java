package org.apache.helix.tools;

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

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

/**
 * Provides ability to start zookeeper locally on a particular port
 */
public class LocalZKServer {
  public void start(int port, String dataDir, String logDir) throws Exception {

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {

      @Override
      public void createDefaultNameSpace(ZkClient zkClient) {

      }
    };
    ZkServer server = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    server.start();
    Thread.currentThread().join();
  }

  public static void main(String[] args) throws Exception {
    int port = 2199;
    String rootDir =
        System.getProperty("java.io.tmpdir") + "/zk-helix/" + System.currentTimeMillis();
    String dataDir = rootDir + "/dataDir";
    String logDir = rootDir + "/logDir";

    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    }
    if (args.length > 1) {
      dataDir = args[1];
      logDir = args[1];
    }

    if (args.length > 2) {
      logDir = args[2];
    }
    System.out.println("Starting Zookeeper locally at port:" + port + " dataDir:" + dataDir
        + " logDir:" + logDir);
    LocalZKServer localZKServer = new LocalZKServer();

    localZKServer.start(port, dataDir, logDir);
  }
}
