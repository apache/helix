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

public class Transition {
  final private String _fromState;
  final private String _toState;

  public Transition(String fromState, String toState) {
    _fromState = fromState;
    _toState = toState;
  }

  @Override
  public String toString() {
    return _fromState + "-" + _toState;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null || !(that instanceof Transition)) {
      return false;
    }
    return this.toString().equalsIgnoreCase(that.toString());
  }

  public String getFromState() {
    return _fromState;
  }

  public String getToState() {
    return _toState;
  }

}
