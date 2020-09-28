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
   * if size of serialized data exceeds the write size limit, which by default is 1 MB or could be
   * set by {@value ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES}.
   * <p>
   * The default value is "true" (enabled).
   */
  public static final String ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED =
      "zk.serializer.znrecord.auto-compress.enabled";

  /**
   * Setting this property to true enables ZNode bucketization, an operation to divide a ZNRecord
   * into specified buckets. This property applies to
   * {@link org.apache.helix.zookeeper.datamodel.ZNRecordBucketizer}.
   * <p>
   * The default value is "true" (enabled).
   */
  public static final String ZK_ZNRECORD_BUCKETIZE_ENABLED =
      "zk.znrecord.bucketize.enabled";

  /**
   * This is property that defines the maximum write size in bytes for ZKRecord's two serializers
   * before serialized data is ready to be written to ZK. This property applies to
   * 1. {@link org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer}
   * 2. {@link org.apache.helix.zookeeper.datamodel.serializer.ZNRecordStreamingSerializer}.
   * <p>
   * If the size of serialized data (no matter whether it is compressed or not) exceeds this
   * configured limit, the data will NOT be written to Zookeeper.
   * <p>
   * Default value is 1 MB. If the configured limit is less than or equal to 0 byte,
   * the default value will be used.
   */
  public static final String ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES =
      "zk.serializer.znrecord.write.size.limit.bytes";

  /**
   * This property determines the behavior of ZkClient issuing an sync() to server upon new session
   * established.
   *
   * <p>
   *   The default value is "true" (issuing sync)
   */
  public static final String ZK_AUTOSYNC_ENABLED =
      "zk.zkclient.autosync.enabled";
}
