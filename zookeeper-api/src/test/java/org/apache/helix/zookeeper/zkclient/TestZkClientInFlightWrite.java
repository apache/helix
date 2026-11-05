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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.concurrent.TimeUnit;

import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.zkclient.ZkClient.InFlightWrite;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit tests for the wait that the synchronous ZkClient writes perform when the calling thread is
 * interrupted while the request is already queued to ZooKeeper. The outcome is driven by hand
 * here so the timing-sensitive cases are deterministic; the end-to-end behavior against a real
 * server is covered in TestRawZkClient.
 */
public class TestZkClientInFlightWrite {
  private static final String PATH = "/in-flight";
  private static final long WAIT_MS = TimeUnit.SECONDS.toMillis(30);

  @BeforeClass
  public void loadZkClient() {
    // Initialize ZkClient (and its logger) before any test interrupts the thread: logging setup
    // on an interrupted thread fails and would poison the class for the rest of the run.
    ZkClient.readInterruptedWriteWaitMs();
  }

  @AfterMethod
  public void clearInterrupt() {
    Thread.interrupted();
  }

  @Test
  public void testOutcomeReturnedWithoutInterrupt() throws Exception {
    InFlightWrite<String> write = new InFlightWrite<>();
    write.complete(KeeperException.Code.OK.intValue(), "result");
    Assert.assertEquals(ZkClient.awaitInFlightWrite(PATH, write, WAIT_MS), "result");
    Assert.assertFalse(Thread.currentThread().isInterrupted());
  }

  @Test
  public void testInterruptedCallerStillGetsSuccess() throws Exception {
    InFlightWrite<String> write = new InFlightWrite<>();
    completeLater(write, KeeperException.Code.OK, "result");
    Thread.currentThread().interrupt();
    Assert.assertEquals(ZkClient.awaitInFlightWrite(PATH, write, WAIT_MS), "result");
    Assert.assertTrue(Thread.interrupted(), "Interrupt flag must be preserved");
  }

  @Test
  public void testInterruptedCallerStillGetsDefiniteFailure() {
    InFlightWrite<String> write = new InFlightWrite<>();
    completeLater(write, KeeperException.Code.NONODE, null);
    Thread.currentThread().interrupt();
    try {
      ZkClient.awaitInFlightWrite(PATH, write, WAIT_MS);
      Assert.fail("Expected NoNodeException");
    } catch (KeeperException.NoNodeException expected) {
      Assert.assertEquals(expected.getPath(), PATH);
    } catch (Exception e) {
      Assert.fail("Expected NoNodeException", e);
    }
    Assert.assertTrue(Thread.interrupted(), "Interrupt flag must be preserved");
  }

  @Test
  public void testInterruptedCallerGivesUpWhenWaitDisabled() {
    InFlightWrite<String> write = new InFlightWrite<>();
    Thread.currentThread().interrupt();
    try {
      ZkClient.awaitInFlightWrite(PATH, write, 0);
      Assert.fail("Expected InterruptedException");
    } catch (InterruptedException expected) {
      // expected
    } catch (Exception e) {
      Assert.fail("Expected InterruptedException", e);
    }
    Assert.assertTrue(Thread.interrupted(), "Interrupt flag must be preserved");
  }

  @Test
  public void testInterruptedCallerGivesUpWhenOutcomeNeverArrives() {
    InFlightWrite<String> write = new InFlightWrite<>();
    Thread.currentThread().interrupt();
    long start = System.nanoTime();
    try {
      ZkClient.awaitInFlightWrite(PATH, write, 200);
      Assert.fail("Expected InterruptedException");
    } catch (InterruptedException expected) {
      // expected
    } catch (Exception e) {
      Assert.fail("Expected InterruptedException", e);
    }
    Assert.assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 200,
        "Must wait out the configured bound before giving up");
    Assert.assertTrue(Thread.interrupted(), "Interrupt flag must be preserved");
  }

  @Test
  public void testSecondInterruptAbortsTheWait() {
    InFlightWrite<String> write = new InFlightWrite<>();
    Thread caller = Thread.currentThread();
    Thread interrupter = new Thread(() -> {
      sleepQuietly(200);
      caller.interrupt();
    });
    interrupter.start();
    Thread.currentThread().interrupt();
    long start = System.nanoTime();
    try {
      ZkClient.awaitInFlightWrite(PATH, write, WAIT_MS);
      Assert.fail("Expected InterruptedException");
    } catch (InterruptedException expected) {
      // expected
    } catch (Exception e) {
      Assert.fail("Expected InterruptedException", e);
    }
    Assert.assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < WAIT_MS / 2,
        "A second interrupt must abort the wait well before the bound");
    Assert.assertTrue(Thread.interrupted(), "Interrupt flag must be preserved");
  }

  @Test
  public void testReadInterruptedWriteWaitMs() {
    String original = System.getProperty(ZkSystemPropertyKeys.ZK_INTERRUPTED_WRITE_WAIT_MS);
    try {
      System.clearProperty(ZkSystemPropertyKeys.ZK_INTERRUPTED_WRITE_WAIT_MS);
      Assert.assertEquals(ZkClient.readInterruptedWriteWaitMs(), -1);
      System.setProperty(ZkSystemPropertyKeys.ZK_INTERRUPTED_WRITE_WAIT_MS, " ");
      Assert.assertEquals(ZkClient.readInterruptedWriteWaitMs(), -1);
      System.setProperty(ZkSystemPropertyKeys.ZK_INTERRUPTED_WRITE_WAIT_MS, "not-a-number");
      Assert.assertEquals(ZkClient.readInterruptedWriteWaitMs(), -1);
      System.setProperty(ZkSystemPropertyKeys.ZK_INTERRUPTED_WRITE_WAIT_MS, "0");
      Assert.assertEquals(ZkClient.readInterruptedWriteWaitMs(), 0);
      System.setProperty(ZkSystemPropertyKeys.ZK_INTERRUPTED_WRITE_WAIT_MS, " 1500 ");
      Assert.assertEquals(ZkClient.readInterruptedWriteWaitMs(), 1500);
    } finally {
      if (original == null) {
        System.clearProperty(ZkSystemPropertyKeys.ZK_INTERRUPTED_WRITE_WAIT_MS);
      } else {
        System.setProperty(ZkSystemPropertyKeys.ZK_INTERRUPTED_WRITE_WAIT_MS, original);
      }
    }
  }

  private static <T> void completeLater(InFlightWrite<T> write, KeeperException.Code code,
      T result) {
    Thread completer = new Thread(() -> {
      sleepQuietly(200);
      write.complete(code.intValue(), result);
    });
    completer.setDaemon(true);
    completer.start();
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
