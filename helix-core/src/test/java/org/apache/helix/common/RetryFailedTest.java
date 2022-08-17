package org.apache.helix.common;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.concurrent.atomic.AtomicInteger;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;


/**
 * A {@link IRetryAnalyzer} that reruns failed test up to MAX_RETRY times.
 */
public class RetryFailedTest implements IRetryAnalyzer {

  private static final int MAX_RETRY = 3;
  private final AtomicInteger _count = new AtomicInteger(0);

  @Override
  public boolean retry(ITestResult testResult) {
    if (testResult.isSuccess()) {
      testResult.setStatus(ITestResult.SKIP);
      return false;
    }
    if (_count.getAndIncrement() < MAX_RETRY) {
      System.out.println("Going to retry failed test " + testResult.getMethod().getMethodName() + ", attempt #" + _count.intValue());
      testResult.getTestContext().getFailedTests().removeResult(testResult);
      return true;
    }
    return false;
  }
}
