package org.apache.helix.store;

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

public class PropertyStat {
  private long _lastModifiedTime; // time in milliseconds from epoch when this property
                                  // was last modified
  private int _version; // latest version number

  public PropertyStat() {
    this(0, 0);
  }

  public PropertyStat(long lastModifiedTime, int version) {
    _lastModifiedTime = lastModifiedTime;
    _version = version;
  }

  public long getLastModifiedTime() {
    return _lastModifiedTime;
  }

  public int getVersion() {
    return _version;
  }

  public void setLastModifiedTime(long lastModifiedTime) {

    _lastModifiedTime = lastModifiedTime;
  }

  public void setVersion(int version) {
    _version = version;
  }
}
