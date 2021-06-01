package org.apache.helix.controller.stages;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.TestHelper;
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAsyncBaseStage {
  private static AsyncWorkerType DEFAULT_WORKER_TYPE = AsyncWorkerType.ExternalViewComputeWorker;
  @Test
  public void testAsyncStageCleanup() throws Exception {
    BlockingAsyncStage blockingAsyncStage = new BlockingAsyncStage();

    Map<AsyncWorkerType, DedupEventProcessor<String, Runnable>> asyncFIFOWorkerPool =
        new HashMap<>();
    DedupEventProcessor<String, Runnable> worker =
        new DedupEventProcessor<String, Runnable>("ClusterName", DEFAULT_WORKER_TYPE.name()) {
          @Override
          protected void handleEvent(Runnable event) {
            event.run();
          }
        };
    worker.start();
    asyncFIFOWorkerPool.put(DEFAULT_WORKER_TYPE, worker);

    ClusterEvent event = new ClusterEvent("ClusterName", ClusterEventType.OnDemandRebalance);
    event.addAttribute(AttributeName.AsyncFIFOWorkerPool.name(), asyncFIFOWorkerPool);

    // Test normal execute case
    blockingAsyncStage.process(event);
    Assert.assertTrue(TestHelper.verify(() -> blockingAsyncStage._isStarted, 500));
    Assert.assertFalse(blockingAsyncStage._isFinished);
    blockingAsyncStage.proceed();
    Assert.assertTrue(TestHelper.verify(() -> blockingAsyncStage._isFinished, 500));
    blockingAsyncStage.reset();

    // Test interruption case
    blockingAsyncStage.process(event);
    TestHelper.verify(() -> blockingAsyncStage._isStarted, 500);
    Assert.assertFalse(blockingAsyncStage._isFinished);
    worker.shutdown();
    Assert.assertFalse(TestHelper.verify(() -> blockingAsyncStage._isFinished, 1000));
    Assert.assertFalse(worker.isAlive());
    blockingAsyncStage.reset();
  }

  private class BlockingAsyncStage extends AbstractAsyncBaseStage {
    public boolean _isFinished = false;
    public boolean _isStarted = false;

    private CountDownLatch _countDownLatch = new CountDownLatch(1);

    public void reset() {
      _isFinished = false;
      _isStarted = false;
      _countDownLatch = new CountDownLatch(1);
    }

    public void proceed() {
      _countDownLatch.countDown();
    }

    @Override
    public AsyncWorkerType getAsyncWorkerType() {
      return DEFAULT_WORKER_TYPE;
    }

    @Override
    public void execute(ClusterEvent event) throws Exception {
      _isStarted = true;
      _countDownLatch.await();
      _isFinished = true;
    }
  }
}
