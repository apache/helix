package org.apache.helix.zookeeper.zkclient;

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

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableSet;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestZkConnection {
  @Test
  public void testGetChildren() throws KeeperException, InterruptedException {
    MockZkConnection zkConnection = new MockZkConnection("dummy.server");
    ZooKeeper zk = mock(ZooKeeper.class);
    zkConnection.setZookeeper(zk);

    List<String> children = Arrays.asList("1", "2", "3");
    when(zk.getChildren(anyString(), anyBoolean())).thenReturn(children);

    List<String> actualChildren = zkConnection.getChildren("/path", false);

    Assert.assertEquals(actualChildren, children);
  }

  @Test
  public void testGetChildrenException() throws KeeperException, InterruptedException {
    MockZkConnection zkConnection = new MockZkConnection("dummy.server");
    ZooKeeper zk = mock(ZooKeeper.class);
    zkConnection.setZookeeper(zk);

    when(zk.getChildren(anyString(), anyBoolean()))
        .thenThrow(KeeperException.create(KeeperException.Code.CONNECTIONLOSS))
        .thenThrow(KeeperException.create(KeeperException.Code.UNIMPLEMENTED))
        .thenThrow(new InterruptedException())
        .thenThrow(new IllegalStateException());

    try {
      zkConnection.getChildren("/path", false);
      Assert.fail("Should have thrown exception.");
    } catch (KeeperException.ConnectionLossException expected) {
      // Expected exception
    }

    try {
      zkConnection.getChildren("/path", false);
      Assert.fail("Should have thrown exception.");
    } catch (KeeperException.ConnectionLossException expected) {
      // Expected exception
    }

    try {
      zkConnection.getChildren("/path", false);
      Assert.fail("Should have thrown exception.");
    } catch (InterruptedException expected) {
      // Expected exception
    }

    try {
      zkConnection.getChildren("/path", false);
      Assert.fail("Should have thrown exception.");
    } catch (RuntimeException expected) {
      // Expected exception
    }
  }

  private static class MockZkConnection extends ZkConnection {

    public MockZkConnection(String zkServers) {
      super(zkServers);
    }

    public void setZookeeper(ZooKeeper zk) {
      _zk = zk;
    }
  }
}
