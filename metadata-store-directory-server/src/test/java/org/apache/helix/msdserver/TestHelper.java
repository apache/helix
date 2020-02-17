package org.apache.helix.msdserver;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.zkclient.IDefaultNameSpace;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestHelper {
  private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);
  public static final long WAIT_DURATION = 20 * 1000L; // 20 seconds

  /**
   * Returns a unused random port.
   */
  public static int getRandomPort() throws IOException {
    ServerSocket sock = new ServerSocket();
    sock.bind(null);
    int port = sock.getLocalPort();
    sock.close();

    return port;
  }

  static public ZkServer startZkServer(final String zkAddress) throws Exception {
    List<String> empty = Collections.emptyList();
    return TestHelper.startZkServer(zkAddress, empty, true);
  }

  static public ZkServer startZkServer(final String zkAddress, final String rootNamespace)
      throws Exception {
    List<String> rootNamespaces = new ArrayList<>();
    rootNamespaces.add(rootNamespace);
    return TestHelper.startZkServer(zkAddress, rootNamespaces, true);
  }

  static public ZkServer startZkServer(final String zkAddress, final List<String> rootNamespaces)
      throws Exception {
    return startZkServer(zkAddress, rootNamespaces, true);
  }

  static public ZkServer startZkServer(final String zkAddress, final List<String> rootNamespaces,
      boolean overwrite) throws Exception {
    System.out.println(
        "Start zookeeper at " + zkAddress + " in thread " + Thread.currentThread().getName());

    String zkDir = zkAddress.replace(':', '_');
    final String logDir = "/tmp/" + zkDir + "/logs";
    final String dataDir = "/tmp/" + zkDir + "/dataDir";
    if (overwrite) {
      FileUtils.deleteDirectory(new File(dataDir));
      FileUtils.deleteDirectory(new File(logDir));
    }

    IDefaultNameSpace defaultNameSpace = zkClient -> {
      if (rootNamespaces == null) {
        return;
      }

      for (String rootNamespace : rootNamespaces) {
        try {
          zkClient.deleteRecursive(rootNamespace);
        } catch (Exception e) {
          LOG.error("fail to deleteRecursive path:" + rootNamespace, e);
        }
      }
    };

    int port = Integer.parseInt(zkAddress.substring(zkAddress.lastIndexOf(':') + 1));
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();

    return zkServer;
  }

  static public void stopZkServer(ZkServer zkServer) {
    if (zkServer != null) {
      zkServer.shutdown();
      System.out.println(
          "Shut down zookeeper at port " + zkServer.getPort() + " in thread " + Thread
              .currentThread().getName());
    }
  }

  public static String getTestMethodName() {
    StackTraceElement[] calls = Thread.currentThread().getStackTrace();
    return calls[2].getMethodName();
  }

  public static String getTestClassName() {
    StackTraceElement[] calls = Thread.currentThread().getStackTrace();
    String fullClassName = calls[2].getClassName();
    return fullClassName.substring(fullClassName.lastIndexOf('.') + 1);
  }

  public interface Verifier {
    boolean verify() throws Exception;
  }

  public static boolean verify(Verifier verifier, long timeout) throws Exception {
    long start = System.currentTimeMillis();
    do {
      boolean result = verifier.verify();
      if (result || (System.currentTimeMillis() - start) > timeout) {
        return result;
      }
      Thread.sleep(50);
    } while (true);
  }
}
