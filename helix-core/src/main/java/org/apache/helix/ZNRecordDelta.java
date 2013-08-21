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

/**
 * A ZNRecord container that specifies how it should be merged with another ZNRecord
 */
public class ZNRecordDelta {
  /**
   * Supported methods of updating a ZNRecord
   */
  public enum MergeOperation {
    ADD,
    SUBTRACT
  };

  /**
   * Backing ZNRecord containing updates
   */
  public ZNRecord _record;

  /**
   * Selected update mode
   */
  public MergeOperation _mergeOperation;

  /**
   * Initialize the delta with a record and the update mode
   * @param record
   * @param _mergeOperation
   */
  public ZNRecordDelta(ZNRecord record, MergeOperation mergeOperation) {
    _record = new ZNRecord(record);
    _mergeOperation = mergeOperation;
  }

  /**
   * Initialize the delta with a record and a default update mode of add
   * @param record
   */
  public ZNRecordDelta(ZNRecord record) {
    this(record, MergeOperation.ADD);
  }

  /**
   * Initialize with an empty ZNRecord and a default update mode of add
   */
  public ZNRecordDelta() {
    this(new ZNRecord(""), MergeOperation.ADD);
  }

  /**
   * Get the backing ZNRecord
   * @return the ZNRecord containing the changes
   */
  public ZNRecord getRecord() {
    return _record;
  }

  /**
   * Get the selected update mode
   * @return MergeOperation currently in effect
   */
  public MergeOperation getMergeOperation() {
    return _mergeOperation;
  }
}
