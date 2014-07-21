package org.apache.helix.testutil;

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
import java.net.ServerSocket;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.NetworkUtil;
import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;

import com.google.common.io.Files;

public class ZkTestUtil {
  static Logger logger = Logger.getLogger(ZkTestUtil.class);
  static final int MAX_PORT = 65535;
  static final int DEFAULT_ZK_START_PORT = 2183;

  public static int availableTcpPort() {
    ServerSocket ss = null;
    try {
      ss = new ServerSocket(0);
      ss.setReuseAddress(true);
      return ss.getLocalPort();
    } catch (IOException e) {

    } finally {
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          // should not be thrown
        }
      }
    }
    return -1;
  }

  public static int availableTcpPort(int startPort) {
    int port = startPort;
    for (; port <= MAX_PORT; port++) {
      if (NetworkUtil.isPortFree(port))
        break;
    }

    return port > MAX_PORT ? -1 : port;
  }

  public static File createAutoDeleteTempDir() {
    File tempdir = Files.createTempDir();
    tempdir.delete();
    tempdir.mkdir();
    logger.info("Create temp dir: " + tempdir.getAbsolutePath());
    tempdir.deleteOnExit();
    return tempdir;
  }

  public static ZkServer startZkServer() {
    File tmpdir = createAutoDeleteTempDir();
    File logdir = new File(tmpdir + File.separator + "translog");
    File datadir = new File(tmpdir + File.separator + "snapshot");

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient) {
        // init any zk paths if needed
      }
    };
    int port = availableTcpPort(DEFAULT_ZK_START_PORT);
    ZkServer zkServer =
        new ZkServer(datadir.getAbsolutePath(), logdir.getAbsolutePath(), defaultNameSpace, port);
    zkServer.start();

    logger.info("Start zookeeper at localhost:" + zkServer.getPort() + " in thread "
        + Thread.currentThread().getName());

    return zkServer;
  }

}
