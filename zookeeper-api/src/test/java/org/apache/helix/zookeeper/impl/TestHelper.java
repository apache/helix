package org.apache.helix.zookeeper.impl;

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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.util.Arrays;

import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


public class TestHelper {
  private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);
  public static final long WAIT_DURATION = 60 * 1000L; // 60 seconds

  /**
   * Returns a unused random port.
   */
  public static int getRandomPort()
      throws IOException {
    ServerSocket sock = new ServerSocket();
    sock.bind(null);
    int port = sock.getLocalPort();
    sock.close();

    return port;
  }

  static public void stopZkServer(ZkServer zkServer) {
    if (zkServer != null) {
      zkServer.shutdown();
      System.out.println(
          "Shut down zookeeper at port " + zkServer.getPort() + " in thread " + Thread
              .currentThread().getName());
    }
  }

  /**
   * generic method for verification with a timeout
   * @param verifierName
   * @param args
   */
  public static void verifyWithTimeout(String verifierName, long timeout, Object... args) {
    final long sleepInterval = 1000; // in ms
    final int loop = (int) (timeout / sleepInterval) + 1;
    try {
      boolean result = false;
      int i = 0;
      for (; i < loop; i++) {
        Thread.sleep(sleepInterval);
        // verifier should be static method
        result = (Boolean) TestHelper.getMethod(verifierName).invoke(null, args);

        if (result) {
          break;
        }
      }

      System.err.println(
          verifierName + ": wait " + ((i + 1) * 1000) + "ms to verify " + " (" + result + ")");
      LOG.debug("args:" + Arrays.toString(args));
      // System.err.println("args:" + Arrays.toString(args));

      if (!result) {
        LOG.error(verifierName + " fails");
        LOG.error("args:" + Arrays.toString(args));
      }

      Assert.assertTrue(result);
    } catch (Exception e) {
      LOG.error("Exception in verify: " + verifierName, e);
    }
  }

  private static Method getMethod(String name) {
    Method[] methods = TestHelper.class.getMethods();
    for (Method method : methods) {
      if (name.equals(method.getName())) {
        return method;
      }
    }
    return null;
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

  public static void resetSystemProperty(String key, String originValue) {
    if (originValue == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, originValue);
    }
  }

  public interface Verifier {
    boolean verify()
        throws Exception;
  }

  public static boolean verify(Verifier verifier, long timeout)
      throws Exception {
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
