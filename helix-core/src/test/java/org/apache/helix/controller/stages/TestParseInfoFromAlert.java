package org.apache.helix.controller.stages;

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

import org.apache.helix.HelixManager;
import org.apache.helix.controller.stages.StatsAggregationStage;
import org.apache.helix.integration.ZkStandAloneCMTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestParseInfoFromAlert extends ZkStandAloneCMTestBase {
  @Test
  public void TestParse() {
    HelixManager manager = _controller;

    String instanceName =
        StatsAggregationStage.parseInstanceName("localhost_12918.TestStat@DB=123.latency", manager);
    Assert.assertTrue(instanceName.equals("localhost_12918"));

    instanceName =
        StatsAggregationStage.parseInstanceName("localhost_12955.TestStat@DB=123.latency", manager);
    Assert.assertTrue(instanceName == null);

    instanceName =
        StatsAggregationStage.parseInstanceName("localhost_12922.TestStat@DB=123.latency", manager);
    Assert.assertTrue(instanceName.equals("localhost_12922"));

    String resourceName =
        StatsAggregationStage.parseResourceName("localhost_12918.TestStat@DB=TestDB.latency",
            manager);
    Assert.assertTrue(resourceName.equals("TestDB"));

    String partitionName =
        StatsAggregationStage.parsePartitionName(
            "localhost_12918.TestStat@DB=TestDB;Partition=TestDB_22.latency",
            manager);
    Assert.assertTrue(partitionName.equals("TestDB_22"));
  }
}
