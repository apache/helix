package org.apache.helix.experiment;

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

import java.util.Map;

public class CapacitySetting {
  private Map<String, Integer> capacity;

  public CapacitySetting(Map<String, Integer> capacity) {
    this.capacity = capacity;
  }

  public CapacitySetting subtract(CapacitySetting other) {
    //TODO
    return null;
  }

  public CapacitySetting add(CapacitySetting other) {
    return null;
  }

  public boolean isValidCapacity() {
    //TODO: check if any negative capacity
    return false;
  }
}
