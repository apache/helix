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
 * Represent a pause in the cluster
 */
public class PauseSignal extends HelixProperty {
  /**
   * Instantiate with an identifier
   * @param id pause signal identifier
   */
  public PauseSignal(String id) {
    super(id);
  }

  /**
   * Instantiate with a pre-populated record
   * @param record ZNRecord with fields corresponding to a pause
   */
  public PauseSignal(ZNRecord record) {
    super(record);
  }

  @Override
  public boolean isValid() {
    return true;
  }
}
