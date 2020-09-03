package org.apache.helix.zookeeper.constant;

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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


/**
 * Constants to be used for testing.
 */
public class TestConstants {
  // ZK hostname prefix and port to be used throughout the zookeeper-api module
  public static final String ZK_PREFIX = "localhost:";
  public static final int ZK_START_PORT = 2127;

  public static final List<String> TEST_KEY_LIST_1 =
      ImmutableList.of("/sharding-key-0", "/sharding-key-1", "/sharding-key-2");
  public static final List<String> TEST_KEY_LIST_2 =
      ImmutableList.of("/sharding-key-3", "/sharding-key-4", "/sharding-key-5");
  public static final List<String> TEST_KEY_LIST_3 =
      ImmutableList.of("/sharding-key-6", "/sharding-key-7", "/sharding-key-8");

  // Based on the ZK hostname constants, construct a set of fake routing data mappings
  public static final Map<String, Collection<String>> FAKE_ROUTING_DATA = ImmutableMap
      .of(ZK_PREFIX + ZK_START_PORT, TEST_KEY_LIST_1, ZK_PREFIX + (ZK_START_PORT + 1),
          TEST_KEY_LIST_2, ZK_PREFIX + (ZK_START_PORT + 2), TEST_KEY_LIST_3);
}
