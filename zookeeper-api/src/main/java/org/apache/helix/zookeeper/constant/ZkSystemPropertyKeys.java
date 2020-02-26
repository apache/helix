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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

public class ZkSystemPropertyKeys {
  /**
   * Setting this property to true in system properties enables auto compression in ZK serializer.
   * The data will be automatically compressed by
   * {@link org.apache.helix.zookeeper.util.GZipCompressionUtil} when being written to Zookeeper
   * if size of serialized data exceeds the configured threshold.
   */
  public static final String ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED =
      "zk.serializer.auto-compression.enabled";

  /**
   * This is property that defines the threshold in bytes for auto compression in ZK serializer.
   * Given auto compression is enabled, if the size of data exceeds this configured threshold,
   * the data will be automatically compressed when being written to Zookeeper.
   */
  public static final String ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES =
      "zk.serializer.auto-compression.threshold.bytes";
}
