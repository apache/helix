package org.apache.helix.messaging.handling;

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

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.collect.ImmutableList;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConfigThreadpoolSize extends ZkStandAloneCMTestBase {
  public static class TestMessagingHandlerFactory implements MultiTypeMessageHandlerFactory {
    public static HashSet<String> _processedMsgIds = new HashSet<String>();

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return null;
    }

    @Override
    public String getMessageType() {
      return "TestMsg";
    }

    @Override public List<String> getMessageTypes() {
      return ImmutableList.of("TestMsg");
    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub
    }

  }

  public static class TestMessagingHandlerFactory2 implements MultiTypeMessageHandlerFactory {
    public static HashSet<String> _processedMsgIds = new HashSet<String>();

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return null;
    }

    @Override
    public String getMessageType() {
      return "TestMsg2";
    }

    @Override public List<String> getMessageTypes() {
      return ImmutableList.of("TestMsg2");
    }

    @Override
    public void reset() {
      // TODO Auto-generated method stub
    }

  }

  @Test
  public void TestThreadPoolSizeConfig() {
    String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + 0);
    HelixManager manager = _participants[0];

    ConfigAccessor accessor = manager.getConfigAccessor();
    ConfigScope scope =
        new ConfigScopeBuilder().forCluster(manager.getClusterName()).forParticipant(instanceName)
            .build();
    accessor.set(scope, "TestMsg." + HelixTaskExecutor.MAX_THREADS, "" + 12);

    scope = new ConfigScopeBuilder().forCluster(manager.getClusterName()).build();
    accessor.set(scope, "TestMsg." + HelixTaskExecutor.MAX_THREADS, "" + 8);

    for (int i = 0; i < NODE_NR; i++) {
      instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);

      _participants[i].getMessagingService().registerMessageHandlerFactory("TestMsg",
          new TestMessagingHandlerFactory());
      _participants[i].getMessagingService()
          .registerMessageHandlerFactory("TestMsg2", new TestMessagingHandlerFactory2());

    }

    for (int i = 0; i < NODE_NR; i++) {
      instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);

      DefaultMessagingService svc =
          (DefaultMessagingService) (_participants[i]
              .getMessagingService());
      HelixTaskExecutor helixExecutor = svc.getExecutor();
      ThreadPoolExecutor executor =
          (ThreadPoolExecutor) (helixExecutor._executorMap.get("TestMsg"));

      ThreadPoolExecutor executor2 =
          (ThreadPoolExecutor) (helixExecutor._executorMap.get("TestMsg2"));
      if (i != 0) {

        Assert.assertEquals(8, executor.getMaximumPoolSize());
      } else {
        Assert.assertEquals(12, executor.getMaximumPoolSize());
      }
      Assert.assertEquals(HelixTaskExecutor.DEFAULT_PARALLEL_TASKS, executor2.getMaximumPoolSize());
    }
  }
}
