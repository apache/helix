package org.apache.helix.ui.api;

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

public class ConfigTableRow implements Comparable<ConfigTableRow> {
  private final String scope;
  private final String entity;
  private final String name;
  private final String value;

  public ConfigTableRow(String scope,
                        String entity,
                        String name,
                        String value) throws Exception {
    this.scope = scope;
    this.entity = entity;
    this.name = name;
    this.value = value;
  }

  public String getScope() {
    return scope;
  }

  public String getEntity() {
    return entity;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  @Override
  public int compareTo(ConfigTableRow o) {
    int nameResult = name.compareTo(o.getName());
    if (nameResult != 0) {
      return nameResult;
    }

    int valueResult = value.compareTo(o.getValue());
    if (valueResult != 0) {
      return valueResult;
    }

    return 0;
  }
}
