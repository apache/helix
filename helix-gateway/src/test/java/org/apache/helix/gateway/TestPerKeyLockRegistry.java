package org.apache.helix.gateway;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.gateway.util.PerKeyLockRegistry;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPerKeyLockRegistry {
  @Test
  public void testConcurrentAccess() {
    PerKeyLockRegistry lockRegistry = new PerKeyLockRegistry();
    final AtomicInteger counter = new AtomicInteger(0);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(2);

    lockRegistry.withLock("key1", () -> {
      counter.incrementAndGet();
      // try to acquir the lock for another key
      lockRegistry.withLock("key2", () -> {
        counter.incrementAndGet();
      });
    });

    // counter should be 2
    Assert.assertEquals(2, counter.get());

    // acquire the lock for key
    ExecutorService executor = Executors.newFixedThreadPool(2);
    lockRegistry.lock("key1");
    executor.submit(() -> {
      lockRegistry.withLock("key1", () -> {
        //try remove lock
        Assert.assertFalse(lockRegistry.removeLock("key1"));
      });
    });
    lockRegistry.unlock("key1");
    executor.submit(() -> {
      lockRegistry.withLock("key2", () -> {
        //try remove lock, should fail because key1 is not locked
        Assert.assertFalse(lockRegistry.removeLock("key1"));
      });
    });
    executor.submit(() -> {
      lockRegistry.withLock("key1", () -> {
        //try remove lock, only this tiem it succeded
        Assert.assertFalse(lockRegistry.removeLock("key1"));
      });
    });
    executor.shutdown();
  }
}
