package org.apache.helix.gateway;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.helix.gateway.util.PerKeyBlockingExecutor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPerKeyBlockingExecutor {
  @Test
  public void testEventNotAddedIfPending() throws InterruptedException {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    CountDownLatch latch3 = new CountDownLatch(1);

    PerKeyBlockingExecutor perKeyBlockingExecutor = new PerKeyBlockingExecutor(3);

    perKeyBlockingExecutor.offerEvent("key1", () -> {
      try {
        latch1.await(); // Wait for the test to release this latch
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    perKeyBlockingExecutor.offerEvent("key1", () -> {
      latch2.countDown();
    });

    Thread.sleep(100); // Give time for the second event to be potentially processed

    Assert.assertFalse(latch2.await(100, TimeUnit.MILLISECONDS)); // Event 2 should not run yet
    latch1.countDown(); // Release the first latch
    Assert.assertTrue(latch2.await(1, TimeUnit.SECONDS)); // Event 2 should run now

    perKeyBlockingExecutor.offerEvent("key1", () -> {
      latch3.countDown();
    });

    Assert.assertTrue(latch3.await(1, TimeUnit.SECONDS)); // Event 3 should run after Event 2
    perKeyBlockingExecutor.shutdown();
  }
}
