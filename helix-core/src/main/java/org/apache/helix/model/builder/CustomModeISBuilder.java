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

import org.apache.helix.model.IdealState;

import java.util.Map;
import java.util.TreeMap;

public class CustomModeISBuilder extends IdealStateBuilder {

    public CustomModeISBuilder(String resourceName)
    {
        super(resourceName);
        setMode(IdealState.IdealStateModeProperty.CUSTOMIZED);
    }

    /**
     * Add a sub-resource
     *
     * @param partitionName
     */
    public void add(String partitionName)
    {
        if (_record.getMapField(partitionName) == null)
        {
            _record.setMapField(partitionName, new TreeMap<String, String>());
        }
    }

    /**
     * add an instance->state assignment
     *
     * @param partitionName
     * @param instanceName
     * @param state
     * @return
     */
    public CustomModeISBuilder assignInstanceAndState(String partitionName, String instanceName,
                                       String state)
    {
        add(partitionName);
        Map<String, String> partitionToInstanceStateMapping = _record
                .getMapField(partitionName);
        partitionToInstanceStateMapping.put(instanceName, state);
        return this;
    }

}
