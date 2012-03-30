package com.linkedin.helix.healthcheck;

import java.util.Random;
import java.util.Timer;

import org.apache.log4j.Logger;

import com.linkedin.helix.ConfigAccessor;
import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixTimerTask;
import com.linkedin.helix.controller.pipeline.Pipeline;
import com.linkedin.helix.controller.stages.ClusterEvent;
import com.linkedin.helix.controller.stages.ReadHealthDataStage;
import com.linkedin.helix.controller.stages.StatsAggregationStage;
import com.linkedin.helix.monitoring.mbeans.ClusterAlertMBeanCollection;
import com.linkedin.helix.monitoring.mbeans.HelixStageLatencyMonitor;

public class HealthStatsAggregationTask extends HelixTimerTask
{
  private static final Logger LOG = Logger.getLogger(HealthStatsAggregationTask.class);

  public final static int DEFAULT_HEALTH_CHECK_LATENCY = 30 * 1000;

  private Timer _timer;
  private final HelixManager _manager;
  private final Pipeline _healthStatsAggregationPipeline;
  private final int _delay;
  private final int _period;
  private final ClusterAlertMBeanCollection _alertItemCollection;
  private HelixStageLatencyMonitor _stageTimeMonitor = null;

  public HealthStatsAggregationTask(HelixManager manager, int delay, int period)
  {
    _manager = manager;
    _delay = delay;
    _period = period;

    // health stats pipeline
    _healthStatsAggregationPipeline = new Pipeline();
    _healthStatsAggregationPipeline.addStage(new ReadHealthDataStage());
    StatsAggregationStage statAggregationStage = new StatsAggregationStage();
    _healthStatsAggregationPipeline.addStage(statAggregationStage);
    _alertItemCollection = statAggregationStage.getClusterAlertMBeanCollection();

    try
    {
      _stageTimeMonitor = new HelixStageLatencyMonitor(_manager.getClusterName());
    }
    catch (Exception e)
    {
      LOG.error("Couldn't create StageTimeMonitor mbean.", e);
    }
  }

  public HealthStatsAggregationTask(HelixManager manager)
  {
    this(manager, DEFAULT_HEALTH_CHECK_LATENCY, DEFAULT_HEALTH_CHECK_LATENCY);
  }

  @Override
  public void start()
  {
    if (!isEnabled())
    {
      LOG.info("HealthAggregationTask is disabled. Will not start the timer");
      return;
    }

    LOG.info("START HealthAggregationTask");

    if (_timer == null)
    {
      _timer = new Timer();
      _timer.scheduleAtFixedRate(this, new Random().nextInt(_delay), _period);
    }
    else
    {
      LOG.warn("timer already started");
    }
  }

  @Override
  public synchronized void stop()
  {
    LOG.info("Stop HealthAggregationTask");

    if (_timer != null)
    {
      _timer.cancel();
      _timer = null;
      _alertItemCollection.reset();
      _stageTimeMonitor.reset();
    }
    else
    {
      LOG.warn("timer already stopped");
    }
  }

  @Override
  public synchronized void run()
  {
    LOG.info("START: HealthStatsAggregationTask");

    if (!_manager.isLeader())
    {
      LOG.error("Cluster manager: " + _manager.getInstanceName()
          + " is not leader. Pipeline will not be invoked");
      return;
    }

    try
    {
      ClusterEvent event = new ClusterEvent("healthChange");
      event.addAttribute("helixmanager", _manager);
      event.addAttribute("HelixStageLatencyMonitor", _stageTimeMonitor);

      _healthStatsAggregationPipeline.handle(event);
      _healthStatsAggregationPipeline.finish();
    }
    catch (Exception e)
    {
      LOG.error("Exception while executing pipeline: " + _healthStatsAggregationPipeline,
                e);
    }

    LOG.info("END: HealthStatsAggregationTask");
  }

  private boolean isEnabled()
  {
    ConfigAccessor configAccessor = _manager.getConfigAccessor();
    boolean enabled = false;
    if (configAccessor != null)
    {
      // zk-based cluster manager
      ConfigScope scope =
          new ConfigScopeBuilder().forCluster(_manager.getClusterName()).build();
      String isEnabled = configAccessor.get(scope, "healthChange.enabled");
      if (isEnabled != null)
      {
        enabled = new Boolean(isEnabled);
      }
    }
    else
    {
      LOG.debug("File-based cluster manager doesn't support disable healthChange");
    }
    return enabled;
  }

}
