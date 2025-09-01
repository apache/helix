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

package org.apache.helix.zookeeper.zkclient;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import static org.mockito.Mockito.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test class for ZkClient retryUntilConnected timeout behavior
 */
public class TestZkClientRetryTimeout extends ZkTestBase {

  private static final long SHORT_RETRY_TIMEOUT = 10000L; // 2 seconds
  private ZkClient _zkClient;

  @BeforeClass
  public void beforeMethod() {
    // Create mock objects
    ZkConnection mockConnection = mock(ZkConnection.class);
    ZooKeeper mockZooKeeper = mock(ZooKeeper.class);

    // Configure mock connection
    when(mockConnection.getServers()).thenReturn("localhost:2181");
    when(mockConnection.getZookeeperState()).thenReturn(ZooKeeper.States.CONNECTED);
    when(mockConnection.getZookeeper()).thenReturn(mockZooKeeper);
    when(mockZooKeeper.getSessionId()).thenReturn(12345L);

    // Create ZkClient with mocked connection
    _zkClient = spy(new ZkClient(mockConnection, ZkClient.DEFAULT_SESSION_TIMEOUT, (int) SHORT_RETRY_TIMEOUT,
        new BasicZkSerializer(new SerializableSerializer()), "TestZkClientRetryTimeout", "test",
        null, false, false, false));

    // Mock getConnection() to return our mocked connection
    when(_zkClient.getConnection()).thenReturn(mockConnection);

    _zkClient.setCurrentState(Watcher.Event.KeeperState.SyncConnected);
  }

  /**
   * Test that retryUntilConnected respects the _operationRetryTimeoutInMillis and throws
   * ZkTimeoutException when the timeout is exceeded.
   */
  @Test
  public void testRetryUntilConnectedTimeoutExceeded() {
    final AtomicInteger retryCount = new AtomicInteger(0);
    final long startTime = System.currentTimeMillis();

    // Create a callable that always throws ConnectionLossException to force retries
    Callable<String> alwaysFailingCallable = () -> {
      retryCount.incrementAndGet();
      // Always throw ConnectionLossException to simulate persistent connection issues
      throw new KeeperException.ConnectionLossException();
    };

    try {
      // This should retry until timeout and then throw ZkTimeoutException
      _zkClient.retryUntilConnected(alwaysFailingCallable);
      Assert.fail("Expected ZkTimeoutException to be thrown after retry timeout");
    } catch (ZkTimeoutException e) {
      long elapsedTime = System.currentTimeMillis() - startTime;

      // Verify that the timeout exception was thrown
      Assert.assertNotNull(e);
      Assert.assertTrue(e.getMessage().contains("retry timeout"));
      Assert.assertTrue(e.getMessage().contains(String.valueOf(SHORT_RETRY_TIMEOUT)));
      Assert.assertTrue(e.getMessage().contains("CONNECTIONLOSS"));

      // Verify that the elapsed time is greater than the retry timeout but within a reasonable range
      Assert.assertTrue(elapsedTime >= SHORT_RETRY_TIMEOUT,
          "Expected elapsed time (" + elapsedTime + "ms) to be >= retry timeout (" + SHORT_RETRY_TIMEOUT + "ms)");
      Assert.assertTrue(elapsedTime < SHORT_RETRY_TIMEOUT * 3,
          "Expected elapsed time (" + elapsedTime + "ms) to be >= retry timeout (" + SHORT_RETRY_TIMEOUT + "ms)");

      // Verify that multiple retry attempts were made
      Assert.assertTrue(retryCount.get() >= 5,
          "Expected multiple retry attempts, but got: " + retryCount.get());
    }
  }

  /**
   * Test that retryUntilConnected works correctly when the operation succeeds before timeout
   */
  @Test
  public void testRetryUntilConnectedSuccessBeforeTimeout() {
    final AtomicInteger retryCount = new AtomicInteger(0);
    final int maxRetriesBeforeSuccess = 3;

    // Create a callable that fails a few times then succeeds
    Callable<String> eventuallySuccessfulCallable = () -> {
      int currentRetry = retryCount.incrementAndGet();

      if (currentRetry <= maxRetriesBeforeSuccess) {
        throw new KeeperException.ConnectionLossException();
      } else {
        return "SUCCESS";
      }
    };

    try {
      String result = _zkClient.retryUntilConnected(eventuallySuccessfulCallable);

      // Verify that the operation eventually succeeded
      Assert.assertEquals(result, "SUCCESS");
      Assert.assertEquals(retryCount.get(), maxRetriesBeforeSuccess + 1);
    } catch (Exception e) {
      Assert.fail("Expected operation to succeed before timeout, but got exception: " + e.getMessage());
    }
  }

  /**
   * Test that retryUntilConnected immediately throws non-retryable exceptions
   */
  @Test
  public void testRetryUntilConnectedNonRetryableException() {
    final AtomicInteger retryCount = new AtomicInteger(0);

    // Create a callable that throws a non-retryable exception
    Callable<String> nonRetryableCallable = () -> {
      retryCount.incrementAndGet();
      // Throw a non-retryable KeeperException
      throw new KeeperException.NoNodeException();
    };

    try {
      _zkClient.retryUntilConnected(nonRetryableCallable);
      Assert.fail("Expected ZkException to be thrown immediately");
    } catch (org.apache.helix.zookeeper.zkclient.exception.ZkException e) {
      // Verify that only one attempt was made (no retries for non-retryable exceptions)
      Assert.assertEquals(retryCount.get(), 1,
          "Expected only one attempt for non-retryable exception, but got: " + retryCount.get());

      // Verify the exception is the expected type
      Assert.assertTrue(e.getCause() instanceof KeeperException.NoNodeException);
    }
  }
}
