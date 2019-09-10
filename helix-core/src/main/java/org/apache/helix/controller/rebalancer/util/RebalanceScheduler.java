package org.apache.helix.controller.rebalancer.util;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;

import org.apache.helix.model.ResourceConfig;
import org.apache.helix.util.RebalanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Class for trigger rebalancing of a set of resource in a future time.
 */
public class RebalanceScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(RebalanceScheduler.class);

  private class ScheduledTask {
    long _startTime;
    Future _future;

    public ScheduledTask(long _startTime, Future _future) {
      this._startTime = _startTime;
      this._future = _future;
    }

    public long getStartTime() {
      return _startTime;
    }

    public Future getFuture() {
      return _future;
    }
  }

  private final Map<String, ScheduledTask> _rebalanceTasks = new HashMap<String, ScheduledTask>();
  private final ScheduledExecutorService _rebalanceExecutor =
      Executors.newSingleThreadScheduledExecutor();

  /**
   * Add a future rebalance task for resource at given startTime
   * @param resource
   * @param startTime time in milliseconds
   */
  public void scheduleRebalance(HelixManager manager, String resource, long startTime) {
    // Do nothing if there is already a timer set for the this workflow with the same start time.
    ScheduledTask existTask = _rebalanceTasks.get(resource);
    if (existTask != null && existTask.getStartTime() == startTime) {
      LOG.debug("Schedule timer for job: {} is up to date.", resource);
      return;
    }

    long delay = startTime - System.currentTimeMillis();
    if (delay < 0) {
      LOG.debug(String.format("Delay time is %s, will not be scheduled", delay));
    }
    LOG.info("Schedule rebalance for resource : {} at time: {} delay: {}", resource, startTime,
        delay);

    // For workflow not yet scheduled, schedule them and record it
    RebalanceInvoker rebalanceInvoker = new RebalanceInvoker(manager, resource);
    ScheduledFuture future =
        _rebalanceExecutor.schedule(rebalanceInvoker, delay, TimeUnit.MILLISECONDS);
    ScheduledTask prevTask = _rebalanceTasks.put(resource, new ScheduledTask(startTime, future));
    if (prevTask != null && !prevTask.getFuture().isDone()) {
      if (!prevTask.getFuture().cancel(false)) {
        LOG.warn("Failed to cancel scheduled timer task for {}", resource);
      }
      LOG.info("Remove previously scheduled timer task for {}", resource);
    }
  }

  /**
   * Get the current schedule time for given resource.
   * @param resource
   * @return existing schedule time or -1 if there is no scheduled task for this resource
   */
  public long getRebalanceTime(String resource) {
    ScheduledTask task = _rebalanceTasks.get(resource);
    if (task != null && !task.getFuture().isDone()) {
      return task.getStartTime();
    }
    return -1;
  }

  /**
   * Remove all existing future schedule tasks for the given resource
   * @param resource
   */
  public long removeScheduledRebalance(String resource) {
    ScheduledTask existTask = _rebalanceTasks.remove(resource);
    if (existTask != null && !existTask.getFuture().isDone()) {
      if (!existTask.getFuture().cancel(true)) {
        LOG.warn("Failed to cancel scheduled timer task for " + resource);
      }
      LOG.info("Remove scheduled rebalance task at time: {} for resource: {}",
          existTask.getStartTime(), resource);

      return existTask.getStartTime();
    }

    return -1;
  }

  /**
   * The simplest possible runnable that will trigger a run of the controller pipeline
   */
  private class RebalanceInvoker implements Runnable {
    private final HelixManager _manager;
    private final String _resource;

    public RebalanceInvoker(HelixManager manager, String resource) {
      _manager = manager;
      _resource = resource;
    }

    @Override
    public void run() {
      RebalanceUtil.scheduleInstantPipeline(_manager.getClusterName());
    }
  }

  /**
   * Trigger the controller to perform rebalance for a given resource.
   * This function is deprecated. Please use RebalanceUtil.scheduleInstantPipeline method instead.
   * @param accessor Helix data accessor
   * @param resource the name of the resource changed to triggering the execution
   */
  @Deprecated
  public static void invokeRebalance(HelixDataAccessor accessor, String resource) {
    LOG.info("invoke rebalance for " + resource);
    PropertyKey key = accessor.keyBuilder().idealStates(resource);
    IdealState is = accessor.getProperty(key);
    if (is != null) {
      // Here it uses the updateProperty function with no-op DataUpdater. Otherwise, it will use default
      // ZNRecordUpdater which will duplicate elements for listFields.
      if (!accessor.updateProperty(key, znRecord -> znRecord, is)) {
        LOG.warn("Failed to invoke rebalance on resource {}", resource);
      }
    } else {
      LOG.warn("Can't find ideal state for {}", resource);
    }
  }

  /**
   * Trigger the controller to perform rebalance for a given resource.
   * This function is deprecated. Please use RebalanceUtil.scheduleInstantPipeline method instead.
   * @param accessor Helix data accessor
   * @param resource the name of the resource changed to triggering the execution
   */
  @Deprecated
  public static void invokeRebalanceForResourceConfig(HelixDataAccessor accessor, String resource) {
    LOG.info("invoke rebalance for " + resource);
    PropertyKey key = accessor.keyBuilder().resourceConfig(resource);
    ResourceConfig cfg = accessor.getProperty(key);
    if (cfg != null) {
      // Here it uses the updateProperty function with no-op DataUpdater. Otherwise, it will use default
      // ZNRecordUpdater which will duplicate elements for listFields.
      if (!accessor.updateProperty(key, znRecord -> znRecord, cfg)) {
        LOG.warn("Failed to invoke rebalance on resource config {}", resource);
      }
    } else {
      LOG.warn("Can't find resource config for {}", resource);
    }
  }
}
