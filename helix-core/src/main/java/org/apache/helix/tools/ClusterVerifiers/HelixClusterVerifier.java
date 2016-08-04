package org.apache.helix.tools.ClusterVerifiers;

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

public interface HelixClusterVerifier {
  /**
   *  Verify the cluster.
   *  The method will be blocked at most {@code timeout}.
   *  Return true if the verify succeed, otherwise return false.
   *
   * @param timeout in milliseconds
   * @return true if succeed, false if not.
   */
  boolean verify(long timeout);

  /**
   *  Verify the cluster.
   *  Return true if the verify succeed, otherwise return false.
   *
   *  @return true if succeed, false if not.
   */
  boolean verify();
}
