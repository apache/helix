package org.apache.helix.metaclient.api;

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

import java.util.List;


public class DirectChildSubscribeResult {
  // A list of direct children names at the time when change is subscribed.
  // It includes only one level child name, does not include further sub children names.
  private final List<String> _children;

  // true means the listener is registered successfully.
  private final boolean _isRegistered;

  public DirectChildSubscribeResult(List<String> children, boolean isRegistered) {
    _children = children;
    _isRegistered = isRegistered;
  }

  public List<String> getDirectChildren() {
    return _children;
  }

  public boolean isRegistered() {
    return _isRegistered;
  }
}
