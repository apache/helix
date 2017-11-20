package org.apache.helix.controller.rebalancer.strategy;

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

/**
 * CRUSH-ed, CRUSH with even distribution. This is an Auto rebalance strategy based on CRUSH algorithm.
 * This gives even partition distribution, but number of partitions to be reshuffled during node outage could be high.
 */
public class CrushEdRebalanceStrategy extends AbstractEvenDistributionRebalanceStrategy {
  private final RebalanceStrategy _baseStrategy = new CrushRebalanceStrategy();

  protected RebalanceStrategy getBaseRebalanceStrategy() {
    return _baseStrategy;
  }
}
