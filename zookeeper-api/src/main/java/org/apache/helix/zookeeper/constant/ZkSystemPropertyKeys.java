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

/**
 * This class contains various ZK system property keys.
 */
public class ZkSystemPropertyKeys {

  /**
   * This is property that defines the threshold in bytes for auto compression in ZKRecord's
   * two serializers:
   * 1. {@link org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer}
   * 2. {@link org.apache.helix.zookeeper.datamodel.serializer.ZNRecordStreamingSerializer}.
   * <p>
   * Given auto compression is enabled, if the size of data exceeds this configured threshold,
   * the data will be automatically compressed when being written to Zookeeper. Default value is
   * 1024000 (1 MB).
   */
  public static final String ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES =
      "znrecord.serializer.compress.threshold.bytes";
}
