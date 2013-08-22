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
 * Defines an error that occurs in computing a valid ideal state or external view
 */
public class Error extends HelixProperty {
  /**
   * Create an error from a record representing an existing one
   * @param record ZNRecord corresponding to an error
   */
  public Error(ZNRecord record) {
    super(record);
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean isValid() {
    // TODO Auto-generated method stub
    return true;
  }
}
