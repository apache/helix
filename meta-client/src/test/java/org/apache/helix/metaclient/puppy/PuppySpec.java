package org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest;

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

/**
 * PuppySpec class definition
 */
public class PuppySpec {
  private final PuppyMode mode;
  private final float errorRate;
  private final ExecDelay execDelay;
  private final int numberDiffPaths;

  public PuppySpec(PuppyMode mode, float errorRate, ExecDelay execDelay, int numberDiffPaths) {
    this.mode = mode;
    this.errorRate = errorRate;
    this.execDelay = execDelay;
    this.numberDiffPaths = numberDiffPaths;
  }

  public PuppyMode getMode() {
    return mode;
  }

  public float getErrorRate() {
    return errorRate;
  }

  public ExecDelay getExecDelay() {
    return execDelay;
  }

  public int getNumberDiffPaths() {
    return numberDiffPaths;
  }
}
