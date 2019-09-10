package org.apache.helix.controller.rebalancer.waged.model;

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

import java.util.function.Supplier;

public interface OnDemandProperty {
  /**
   * If the specified key is not already associated with a value (or is mapped to null), attempts to
   * compute its value using the given compute function and enters it into this map unless null.
   * @param key key with which the specified value is to be associated
   * @param computeFunction The function to compute a value
   * @param classType The class definition of target type
   * @param <T>
   * @return the current (existing or computed) value associated with the specified key, or null if
   *         the computed value is null
   */
  <T> T getOrCompute(String key, Supplier<T> computeFunction, Class<T> classType);
}
