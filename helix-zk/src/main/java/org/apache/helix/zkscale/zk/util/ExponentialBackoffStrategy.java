package org.apache.helix.zkscale.zk.util;

import java.util.Random;

public class ExponentialBackoffStrategy {
  private final long INIT_RETRY_INTERVAL = 500;
  private final long _maxRetryInterval;
  private final boolean _addJitter;
  private final Random _ran;

  public ExponentialBackoffStrategy(long maxRetryInterval, boolean addJitter) {
    _maxRetryInterval = maxRetryInterval;
    _addJitter = addJitter;
    _ran = new Random(System.currentTimeMillis());
  }

  public long getNextWaitInterval(int numberOfTriesFailed) {
    double exponentialMultiplier = Math.pow(2.0, numberOfTriesFailed - 1);
    double result = exponentialMultiplier * INIT_RETRY_INTERVAL;

    if (_maxRetryInterval > 0 && result > _maxRetryInterval) {
      result = _maxRetryInterval;
    }

    if (_addJitter) {
      // Adding jitter so the real result would be 75% to 100% of the original result.
      // Don't directly add jitter here, since it may exceed the max retry interval setup
      result = result * (0.75 + _ran.nextDouble() % 0.25);
    }

    return (long) result;
  }
}

