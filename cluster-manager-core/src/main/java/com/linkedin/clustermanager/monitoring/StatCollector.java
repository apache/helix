package com.linkedin.clustermanager.monitoring;

import java.util.concurrent.TimeUnit;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class StatCollector
{
  private static final int DEFAULT_WINDOW_SIZE = 4000;
  private final DescriptiveStatistics _stats;
  private long _numDataPoints;
  private long _totalSum;
  
  public StatCollector()
  {
    _stats = new DescriptiveStatistics();
    _stats.setWindowSize(DEFAULT_WINDOW_SIZE);
  }
  
  public void addData(double data)
  {
    _numDataPoints++;
    _totalSum += data;
    _stats.addValue(data);
  }
  
  public long getTotalSum()
  {
    return _totalSum;
  }

  public DescriptiveStatistics getStatistics()
  {
    return _stats;
  }

  public long getNumDataPoints()
  {
    return _numDataPoints;
  }
  
  public void reset()
  {
    _numDataPoints = 0;
    _totalSum = 0;
    _stats.clear();
  }
  
  public double getMean()
  {
    return _stats.getMean();
  }

  public double getMax()
  {
    // TODO Auto-generated method stub
    return _stats.getMax();
  }

  public double getMin()
  {
    // TODO Auto-generated method stub
    return _stats.getMin();
  }

  public double getPercentile(int percentage)
  {
    // TODO Auto-generated method stub
    return _stats.getPercentile(percentage*1.0 / 100);
  }
}
