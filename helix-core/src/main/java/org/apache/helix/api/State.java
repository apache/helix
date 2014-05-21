package org.apache.helix.api;

import org.apache.helix.HelixDefinedState;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

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
 * A state in a state model managed by Helix
 */
public class State {
  @JsonProperty("state")
  private final String _state;

  /**
   * Create a state
   * @param state string representing a state
   */
  @JsonCreator
  public State(@JsonProperty("state") String state) {
    _state = state.toUpperCase();
  }

  @Override
  public String toString() {
    return _state;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof State) {
      return this.toString().equalsIgnoreCase(((State) that).toString());
    } else if (that instanceof String) {
      return _state.equalsIgnoreCase(that.toString());
    } else if (that instanceof HelixDefinedState) {
      return _state.equalsIgnoreCase(that.toString());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return _state.hashCode();
  }

  /**
   * Get a State from a state name
   * @param state state name
   * @return State
   */
  public static State from(String state) {
    if (state == null) {
      return null;
    }
    return new State(state);
  }

  /**
   * Get a State from a HelixDefinedState
   * @param state HelixDefinedState
   * @return State
   */
  public static State from(HelixDefinedState state) {
    if (state == null) {
      return null;
    }
    return new State(state.toString());
  }
}
