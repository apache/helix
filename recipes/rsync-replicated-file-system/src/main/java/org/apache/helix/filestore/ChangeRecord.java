package org.apache.helix.filestore;

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

public class ChangeRecord {
  /**
   * Transaction Id corresponding to the change that increases monotonically. 31
   * LSB correspond to sequence number that increments every change. 31 MSB
   * increments when the master changes
   */
  long txid;
  /**
   * File(s) that were changed
   */
  String file;

  /**
   * Timestamp
   */
  long timestamp;
  /**
   * Type of event like create, modified, deleted
   */
  short type;

  transient String changeLogFileName;

  transient long startOffset;

  transient long endOffset;

  public short fileFieldLength;

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(txid);
    sb.append("|");
    sb.append(timestamp);
    sb.append("|");
    sb.append(file);
    sb.append("|");
    sb.append(changeLogFileName);
    sb.append("|");
    sb.append(startOffset);
    sb.append("|");
    sb.append(endOffset);
    return sb.toString();
  }

  public static ChangeRecord fromString(String line) {
    ChangeRecord record = null;
    if (line != null) {
      String[] split = line.split("\\|");
      if (split.length == 6) {
        record = new ChangeRecord();
        record.txid = Long.parseLong(split[0]);
        record.timestamp = Long.parseLong(split[1]);
        record.file = split[2];
        record.changeLogFileName = split[3];
        record.startOffset = Long.parseLong(split[4]);
        record.endOffset = Long.parseLong(split[5]);
      }
    }
    return record;
  }

}
