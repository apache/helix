package org.apache.helix.monitoring;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

public class StatCollector {
  private static final int DEFAULT_WINDOW_SIZE = 100;
  private final DescriptiveStatistics _stats;
  private long _numDataPoints;
  private long _totalSum;

  public StatCollector() {
    _stats = new SynchronizedDescriptiveStatistics();
    _stats.setWindowSize(DEFAULT_WINDOW_SIZE);
  }

  public void addData(double data) {
    _numDataPoints++;
    _totalSum += data;
    _stats.addValue(data);
  }

  public long getTotalSum() {
    return _totalSum;
  }

  public DescriptiveStatistics getStatistics() {
    return _stats;
  }

  public long getNumDataPoints() {
    return _numDataPoints;
  }

  public void reset() {
    _numDataPoints = 0;
    _totalSum = 0;
    _stats.clear();
  }

  public double getMean() {
    if (_stats.getN() == 0) {
      return 0;
    }
    return _stats.getMean();
  }

  public double getMax() {
    return _stats.getMax();
  }

  public double getMin() {
    return _stats.getMin();
  }

  public double getPercentile(int percentage) {
    if (_stats.getN() == 0) {
      return 0;
    }
    return _stats.getPercentile(percentage * 1.0);
  }
}
