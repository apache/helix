package com.linkedin.helix.controller.pipeline;

import java.util.Map;

import com.linkedin.helix.controller.stages.ClusterEvent;
import com.linkedin.helix.monitoring.mbeans.HelixStageLatencyMonitor;

public class AbstractBaseStage implements Stage
{
  @Override
  public void init(StageContext context)
  {

  }

  @Override
  public void preProcess()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void process(ClusterEvent event) throws Exception
  {

  }

  @Override
  public void postProcess()
  {

  }

  @Override
  public void release()
  {

  }

  @Override
  public String getStageName()
  {
    // default stage name will be the class name
    String className = this.getClass().getName();
    return className;
  }

  public void addLatencyToMonitor(ClusterEvent event, long latency)
  {
    Map<String, HelixStageLatencyMonitor> stgLatencyMonitorMap =
        event.getAttribute("HelixStageLatencyMonitorMap");
    if (stgLatencyMonitorMap != null)
    {
      if (stgLatencyMonitorMap.containsKey(getStageName()))
      {
        HelixStageLatencyMonitor stgLatencyMonitor =
            stgLatencyMonitorMap.get(getStageName());
        stgLatencyMonitor.addStgLatency(latency);
      }
    }
  }
}
