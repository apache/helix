package org.apache.helix.task.beans;

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

import java.util.List;

import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.WorkflowConfig;

/**
 * Bean class used for parsing workflow definitions from YAML.
 */
public class WorkflowBean {
  public String name;
  public List<JobBean> jobs;
  public ScheduleBean schedule;
  public long expiry = WorkflowConfig.DEFAULT_EXPIRY;
  public String workflowType;
  public boolean enableCompression = TaskConstants.DEFAULT_TASK_ENABLE_COMPRESSION;
}
