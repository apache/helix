package org.apache.helix.api.id;

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

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Identifies a context
 */
public class ContextId extends Id {
  @JsonProperty("id")
  final private String _id;

  /**
   * Create a context id
   * @param id string representation of the id
   */
  @JsonCreator
  public ContextId(@JsonProperty("id") String id) {
    _id = id;
  }

  @Override
  public String stringify() {
    return _id;
  }

  /**
   * Get a concrete context id for a string name
   * @param contextId string context identifier
   * @return ContextId
   */
  public static ContextId from(String contextId) {
    if (contextId == null) {
      return null;
    }
    return new ContextId(contextId);
  }
}
