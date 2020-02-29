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
   * Setting this property to true in system properties enables auto compression in ZK serializer.
   * The data will be automatically compressed by
   * {@link org.apache.helix.zookeeper.util.GZipCompressionUtil} when being written to Zookeeper
   * if size of serialized data exceeds the write size limit. The default value is enabled.
   */
  public static final String ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED =
      "zk.serializer.znrecord.auto-compress.enabled";

  /**
   * This is property that defines the maximum write size in bytes for ZKRecord's two serializers
   * before serialized data is ready to be written to ZK. This property applies to
   * 1. {@link org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer}
   * 2. {@link org.apache.helix.zookeeper.datamodel.serializer.ZNRecordStreamingSerializer}.
   * <p>
   * If the size of serialized data exceeds this configured limit, the data will NOT be written
   * to Zookeeper. Default value is 1 MB. If the configured limit is greater than
   * 1 MB or less than or equal to 0 byte, 1 MB will be used.
   */
  public static final String ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES =
      "zk.serializer.znrecord.write.size.limit.bytes";
}
