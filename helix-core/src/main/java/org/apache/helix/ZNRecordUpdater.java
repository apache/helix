package org.apache.helix;

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

import org.I0Itec.zkclient.DataUpdater;

/**
 * Class that specifies how a ZNRecord should be updated with another ZNRecord
 */
public class ZNRecordUpdater implements DataUpdater<ZNRecord> {
  final ZNRecord _record;

  /**
   * Initialize with the record that will be updated
   * @param record
   */
  public ZNRecordUpdater(ZNRecord record) {
    _record = record;
  }

  @Override
  public ZNRecord update(ZNRecord current) {
    if (current != null) {
      current.merge(_record);
      return current;
    }
    return _record;
  }
}
