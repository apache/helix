package org.apache.helix.api;

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
 * Helix version (e.g. 0.6.1.5)
 */
public class HelixVersion {
  final String _version;

  /**
   * Construct with a version string (e.g. 0.6.1.5)
   * @param version
   */
  public HelixVersion(String version) {
    _version = version;
  }

  /**
   * Get major version (e.g. 6 in 0.6.1.5)
   * @return major version number
   */
  public String getMajor() {
    return null;
  }

  /**
   * Get minor version (e.g. 1 in 0.6.1.5)
   * @return minor version number
   */
  public String getMinor() {
    return null;
  }

  @Override
  public String toString() {
    return _version;
  }

  /**
   * Create a version from a version string
   * @param version string in the form of a.b.c.d
   * @return HelixVersion
   */
  public static HelixVersion from(String version) {
    if (version == null) {
      return null;
    }
    return new HelixVersion(version);
  }
}
