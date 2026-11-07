package org.apache.helix.controller.stages;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixManager;
import org.apache.helix.TestHelper;
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.mockito.Mockito;
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

  @Test
  public void testStaleSessionEventIsSkipped() throws Exception {
    DedupEventProcessor<String, Runnable> worker =
        new DedupEventProcessor<String, Runnable>("ClusterName", DEFAULT_WORKER_TYPE.name()) {
          @Override
          protected void handleEvent(Runnable event) {
            event.run();
          }
        };
    worker.start();
    Map<AsyncWorkerType, DedupEventProcessor<String, Runnable>> asyncFIFOWorkerPool =
        new HashMap<>();
    asyncFIFOWorkerPool.put(DEFAULT_WORKER_TYPE, worker);

    HelixManager manager = Mockito.mock(HelixManager.class);
    Mockito.when(manager.isConnected()).thenReturn(true);
    Mockito.when(manager.getSessionId()).thenReturn("session-2");
    Mockito.when(manager.getInstanceName()).thenReturn("controller");

    try {
      // The session the event was created in has expired by the time the task runs: skip it.
      RecordingAsyncStage stage = new RecordingAsyncStage();
      stage.process(event(asyncFIFOWorkerPool, manager, Optional.of("session-1")));
      waitForWorker(worker);
      Assert.assertFalse(stage._executed, "A task for a stale event must not be executed");

      // The event's session is still the controller's session: execute it.
      stage = new RecordingAsyncStage();
      stage.process(event(asyncFIFOWorkerPool, manager, Optional.of("session-2")));
      waitForWorker(worker);
      Assert.assertTrue(stage._executed, "A task for a current event must be executed");

      // While the manager is disconnected the session is unknown: keep the previous behavior.
      Mockito.when(manager.isConnected()).thenReturn(false);
      stage = new RecordingAsyncStage();
      stage.process(event(asyncFIFOWorkerPool, manager, Optional.of("session-1")));
      waitForWorker(worker);
      Assert.assertTrue(stage._executed,
          "A task must be executed while the session cannot be determined");
    } finally {
      worker.shutdown();
    }
  }

  private static ClusterEvent event(
      Map<AsyncWorkerType, DedupEventProcessor<String, Runnable>> asyncFIFOWorkerPool,
      HelixManager manager, Optional<String> eventSession) {
    ClusterEvent event = new ClusterEvent("ClusterName", ClusterEventType.OnDemandRebalance);
    event.addAttribute(AttributeName.AsyncFIFOWorkerPool.name(), asyncFIFOWorkerPool);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.EVENT_SESSION.name(), eventSession);
    return event;
  }

  /**
   * Queues a marker task behind whatever the worker is processing and waits for it, so that every
   * task queued before it has been handled (or skipped) once this returns.
   */
  private static void waitForWorker(DedupEventProcessor<String, Runnable> worker)
      throws InterruptedException {
    CountDownLatch drained = new CountDownLatch(1);
    worker.queueEvent("marker", drained::countDown);
    Assert.assertTrue(drained.await(5, TimeUnit.SECONDS), "Worker did not drain its queue");
  }

  private class RecordingAsyncStage extends AbstractAsyncBaseStage {
    private volatile boolean _executed = false;

    @Override
    public AsyncWorkerType getAsyncWorkerType() {
      return DEFAULT_WORKER_TYPE;
    }

    @Override
    public void execute(ClusterEvent event) {
      _executed = true;
    }
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
