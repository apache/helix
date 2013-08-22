package org.apache.helix.model;

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

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

/**
 * Wraps updates to Helix constructs, e.g. state transitions and controller task statuses
 */
public class StatusUpdate extends HelixProperty {

  /**
   * Instantiate with a pre-populated record
   * @param record ZNRecord corresponding to a status update
   */
  public StatusUpdate(ZNRecord record) {
    super(record);
  }

  @Override
  public boolean isValid() {
    return true;
  }

}
