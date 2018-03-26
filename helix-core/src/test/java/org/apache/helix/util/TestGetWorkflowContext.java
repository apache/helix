package org.apache.helix.util;

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

import com.google.common.base.Joiner;
import org.apache.helix.AccessOption;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestGetWorkflowContext extends TaskTestBase {

  private static final String WORKFLOW_NAME = "testWorkflow_01";
  private static final String CONTEXT_NODE = "Context";

  /**
   * This test method tests whether PropertyKey.Builder successfully creates a path for WorkflowContext instances.
   * TODO: KeyBuilder must handle the case for future versions of Task Framework with a different path structure
   */
  @Test
  public void testGetWorkflowContext() {
    // Manually create a WorkflowContext instance
    ZNRecord znRecord = new ZNRecord(WORKFLOW_NAME);
    WorkflowContext workflowContext = new WorkflowContext(znRecord);
    _manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, WORKFLOW_NAME, CONTEXT_NODE),
        workflowContext.getRecord(), AccessOption.PERSISTENT);

    // Test retrieving this WorkflowContext using PropertyKey.Builder.getPath()
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);
    String path = keyBuilder.workflowContext(WORKFLOW_NAME).getPath();
    WorkflowContext workflowCtx = new WorkflowContext(_baseAccessor.get(path, null, AccessOption.PERSISTENT));

    Assert.assertEquals(workflowContext, workflowCtx);
  }
}