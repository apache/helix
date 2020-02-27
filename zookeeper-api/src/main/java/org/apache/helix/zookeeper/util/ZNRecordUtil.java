package org.apache.helix.zookeeper.util;

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

import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * This utility class contains various methods for manipulating ZNRecord.
 */
public class ZNRecordUtil {

  /**
   * Checks whether or not a serialized ZNRecord bytes should be compressed before being written to
   * Zookeeper.
   *
   * @param record raw ZNRecord before being serialized
   * @param serializedLength length of the serialized bytes array
   * @return
   */
  public static boolean shouldCompress(ZNRecord record, int serializedLength) {
    if (record.getBooleanField(ZNRecord.ENABLE_COMPRESSION_BOOLEAN_FIELD, false)) {
      return true;
    }

    return serializedLength > getCompressThreshold();
  }

  /**
   * Returns compression threshold in bytes. The threshold is a smaller number determined by the
   * configured threshold and {@link ZNRecord#SIZE_LIMIT}.
   */
  public static int getCompressThreshold() {
    Integer threshold =
        Integer.getInteger(ZkSystemPropertyKeys.ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES);
    if (threshold == null || threshold <= 0 || threshold > ZNRecord.SIZE_LIMIT) {
      return ZNRecord.SIZE_LIMIT;
    }
    return threshold;
  }
}
