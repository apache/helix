package com.linkedin.helix.monitoring;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

public class StatCollector
{
  private static final int DEFAULT_WINDOW_SIZE = 100;
  private final DescriptiveStatistics _stats;
  private long _numDataPoints;
  private long _totalSum;

  public StatCollector()
  {
    _stats = new SynchronizedDescriptiveStatistics();
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
    if(_stats.getN() == 0)
    {
      return 0;
    }
    return _stats.getMean();
  }

  public double getMax()
  {
    return _stats.getMax();
  }

  public double getMin()
  {
    return _stats.getMin();
  }

  public double getPercentile(int percentage)
  {
    if(_stats.getN() == 0)
    {
      return 0;
    }
    return _stats.getPercentile(percentage*1.0);
  }
}
