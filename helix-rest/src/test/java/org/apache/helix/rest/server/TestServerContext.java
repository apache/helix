package org.apache.helix.rest.server;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.rest.server.resources.helix.PropertyStoreAccessor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


public class TestServerContext extends AbstractTestClass {
  private static class ServerContextAccessor extends ServerContext {
    ServerContextAccessor(String zkAddr) {
      super(zkAddr);
    }

    Map<ZkSerializer, ZkBaseDataAccessor<ZNRecord>> getZkBaseDataAccessors() {
      return _zkBaseDataAccessorBySerializer;
    }
  }

  private ServerContextAccessor _serverContext = new ServerContextAccessor(ZK_ADDR);

  @Test
  public void testGetZkBaseDataAccessorWithPropertyStoreSerializer()
      throws InterruptedException {
    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      String threadName = "Thread: " + i;
      // each thread tries to get an instance of {@link ZkBaseDataAccessor}
      // with different instances of {@link PropertyStoreSerializer}
      threads[i] = new Thread(() -> {
        ZkSerializer zkSerializer = new PropertyStoreAccessor.PropertyStoreSerializer(threadName);
        _serverContext.getZkBaseDataAccessor(zkSerializer);
      }, threadName);
      threads[i].start();
    }
    for (Thread thread : threads) {
      thread.join();
    }

    // expect only one instance of {@link ZkBaseDataAccessor}
    Assert.assertEquals(_serverContext.getZkBaseDataAccessors().size(), 1);
  }

  @AfterClass
  public void close() {
    _serverContext.close();
  }
}
