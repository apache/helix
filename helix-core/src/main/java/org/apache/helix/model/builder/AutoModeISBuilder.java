package org.apache.helix.model.builder;

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

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.helix.model.IdealState.RebalanceMode;

/**
 * This is the deprecated IS builder for SEMI-AUTO rebalance mode. Please use SemiAutoISBuilder instead.
 */
@Deprecated
public class AutoModeISBuilder extends IdealStateBuilder {
  public AutoModeISBuilder(String resourceName) {
    super(resourceName);
    setRebalancerMode(RebalanceMode.SEMI_AUTO);
  }

  public void add(String partitionName) {
    if (_record.getListField(partitionName) == null) {
      _record.setListField(partitionName, new ArrayList<String>());
    }
  }

  public AutoModeISBuilder assignPreferenceList(String partitionName, String... instanceNames) {
    add(partitionName);
    _record.getListField(partitionName).addAll(Arrays.asList(instanceNames));
    return this;
  }

}
