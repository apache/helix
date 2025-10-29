package org.apache.helix.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hybrid version:
 * - Single shared thread pool reused by all Helix stages.
 * - Per-stage thread name prefixes for better debugging.
 * - Ensures all stage tasks complete before next stage runs.
 */
public class StageThreadPoolHelper {
  private static final Logger LOG = LoggerFactory.getLogger(StageThreadPoolHelper.class);

  private static final int DEFAULT_POOL_SIZE =
      Math.min(4, Runtime.getRuntime().availableProcessors());
  private static final long THREAD_KEEP_ALIVE_MIN = 3;
  private static final String THREAD_NAME_PREFIX = "HelixStageWorker";

  // Shared executor reused across all stages
  private static final AtomicReference<ThreadPoolExecutor> EXECUTOR_REF = new AtomicReference<>();

  private StageThreadPoolHelper() {
  }

  /**
   * Lazily initialize shared thread pool.
   */
  private static synchronized ThreadPoolExecutor getExecutor() {
    ThreadPoolExecutor existing = EXECUTOR_REF.get();
    if (existing != null && !existing.isShutdown()) {
      return existing;
    }

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat(THREAD_NAME_PREFIX + "-%d")
        .setDaemon(true)
        .setUncaughtExceptionHandler((t, e) ->
            LOG.error("Uncaught exception in stage thread {}", t.getName(), e))
        .build();

    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        DEFAULT_POOL_SIZE,
        DEFAULT_POOL_SIZE,
        THREAD_KEEP_ALIVE_MIN,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(),
        threadFactory,
        new ThreadPoolExecutor.CallerRunsPolicy());

    executor.allowCoreThreadTimeOut(true);
    EXECUTOR_REF.set(executor);
    LOG.info("Initialized shared stage parallel executor with {} threads", DEFAULT_POOL_SIZE);
    return executor;
  }

  /**
   * Execute multiple tasks in parallel for a stage and wait for all to complete.
   *
   * @param stageName Name of the stage for logging/thread naming.
   * @param tasks     Tasks to execute.
   */
  public static void executeAndWait(String stageName, Collection<? extends Callable<?>> tasks)
      throws InterruptedException {
    if (tasks == null || tasks.isEmpty()) {
      return;
    }

    ThreadPoolExecutor executor = getExecutor();
    List<Future<?>> futures = new ArrayList<>(tasks.size());

    for (Callable<?> task : tasks) {
      futures.add(executor.submit(wrapWithStageContext(task, stageName)));
    }

    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.warn("Task in stage {} failed: {}", stageName, e.getCause().getMessage(), e);
      }
    }

    LOG.debug("Completed parallel execution for stage {}", stageName);
  }

  /**
   * Gracefully shutdown the shared executor.
   * Should be called once when controller stops.
   */
  public static synchronized void shutdown() {
    ThreadPoolExecutor executor = EXECUTOR_REF.get();
    if (executor == null || executor.isShutdown()) {
      return;
    }

    LOG.info("Shutting down shared stage parallel executor");
    executor.shutdown();
    try {
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("Executor did not terminate gracefully, forcing shutdown");
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted during shutdown", e);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Wraps the callable to temporarily rename the current thread
   * with stage-specific prefix during execution (for debugging).
   */
  private static Callable<?> wrapWithStageContext(Callable<?> task, String stageName) {
    return () -> {
      Thread current = Thread.currentThread();
      String originalName = current.getName();
      current.setName(stageName + "-" + originalName);
      try {
        return task.call();
      } finally {
        current.setName(originalName);
      }
    };
  }
}