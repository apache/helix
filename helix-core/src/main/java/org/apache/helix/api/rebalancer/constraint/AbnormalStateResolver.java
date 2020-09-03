package org.apache.helix.api.rebalancer.constraint;

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
import java.util.Map;

import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;

/**
 * A generic interface to find and recover if the partition has abnormal current states.
 */
public interface AbnormalStateResolver {
  /**
   * Check if the current states of the specified partition is valid.
   * @param currentStateOutput
   * @param resourceName
   * @param partition
   * @param stateModelDef
   * @return true if the current states of the specified partition is valid.
   */
  boolean checkCurrentStates(final CurrentStateOutput currentStateOutput,
      final String resourceName, final Partition partition,
      final StateModelDefinition stateModelDef);

  /**
   * Compute a transient partition state assignment to fix the abnormal.
   * @param currentStateOutput
   * @param resourceName
   * @param partition
   * @param stateModelDef
   * @param preferenceList
   * @return the transient partition state assignment which remove the abnormal states.
   */
  Map<String, String> computeRecoveryAssignment(final CurrentStateOutput currentStateOutput,
      final String resourceName, final Partition partition,
      final StateModelDefinition stateModelDef, final List<String> preferenceList);
}
