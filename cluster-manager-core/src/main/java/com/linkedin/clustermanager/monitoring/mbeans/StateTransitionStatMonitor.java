package com.linkedin.clustermanager.monitoring.mbeans;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import com.linkedin.clustermanager.monitoring.StatCollector;
import com.linkedin.clustermanager.monitoring.StateTransitionContext;
import com.linkedin.clustermanager.monitoring.StateTransitionDataPoint;

public class StateTransitionStatMonitor implements StateTransitionStatMonitorMBean
{
  public enum LATENCY_TYPE {TOTAL, EXECUTION};
  
  private static final int DEFAULT_WINDOW_SIZE = 4000;
  private long _numDataPoints;
  private long _successCount;
  private TimeUnit _unit;
  
  private ConcurrentHashMap<LATENCY_TYPE, StatCollector> _monitorMap
     = new ConcurrentHashMap<LATENCY_TYPE, StatCollector>();
  
  StateTransitionContext _context;
  
  public StateTransitionStatMonitor(StateTransitionContext context, TimeUnit unit)
  {
    _context = context;
    _monitorMap.put(LATENCY_TYPE.TOTAL, new StatCollector());
    _monitorMap.put(LATENCY_TYPE.EXECUTION, new StatCollector());
    reset();
  }
  
  public void addDataPoint(StateTransitionDataPoint data)
  {
    _numDataPoints++;
    if(data.getSuccess())
    {
      _successCount++;
    }
    // should we count only the transition time for successful transitions?
    addLatency(LATENCY_TYPE.TOTAL, data.getTotalDelay());
    addLatency(LATENCY_TYPE.EXECUTION, data.getExecutionDelay());
  }
  
  void addLatency(LATENCY_TYPE type, double latency)
  {
    assert(_monitorMap.containsKey(type));
    _monitorMap.get(type).addData(latency);
  }
  
  public long getNumDataPoints()
  {
    return _numDataPoints;
  }
  
  public void reset()
  {
    _numDataPoints = 0;
    _successCount = 0;
    for(StatCollector monitor : _monitorMap.values())
    {
      monitor.reset();
    }
  }

  @Override
  public long getTotalStateTransitions()
  {
    return _numDataPoints;
  }

  @Override
  public long getTotalFailedTransitions()
  {
    return _numDataPoints - _successCount;
  }

  @Override
  public long getTotalSuccessTransitions()
  {
    return _successCount;
  }

  @Override
  public double getMeanTransitionLatency()
  {
    return _monitorMap.get(LATENCY_TYPE.TOTAL).getMean();
  }

  @Override
  public double getMaxTransitionLatency()
  {
    return _monitorMap.get(LATENCY_TYPE.TOTAL).getMax();
  }

  @Override
  public double getMinTransitionLatency()
  {
    return _monitorMap.get(LATENCY_TYPE.TOTAL).getMin();
  }

  @Override
  public double getPercentileTransitionLatency(int percentage)
  {
    return _monitorMap.get(LATENCY_TYPE.TOTAL).getPercentile(percentage);
  }
}
