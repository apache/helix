package org.apache.helix.task;

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

/**
 * Constants used in the task framework.
 */
public class TaskConstants {
  /**
   * The name of the {@link Task} state model.
   */
  public static final String STATE_MODEL_NAME = "Task";
  /**
   * Field in workflow resource config housing dag
   */
  public static final String WORKFLOW_DAG_FIELD = "dag";
  /**
   * Field in workflow resource config for flow name
   */
  public static final String WORKFLOW_NAME_FIELD = "name";
  /**
   * The root property store path at which the {@link TaskRebalancer} stores context information.
   */
  public static final String REBALANCER_CONTEXT_ROOT = "/TaskRebalancer";
  /**
   * The context node for workflow and job
   */
  public static final String CONTEXT_NODE = "Context";

  public static final long DEFAULT_NEVER_TIMEOUT = -1; // never timeout

  public static final String PREV_RA_NODE = "PreviousResourceAssignment";

  public static final boolean DEFAULT_TASK_ENABLE_COMPRESSION = false;
  /**
   * Name of the JAR file for the task dynamically loaded in {@link TaskStateModel}
   */
  public static final String TASK_JAR_FILE_KEY = "JAR_FILE";
  /**
   * Version of the task dynamically loaded in {@link TaskStateModel}
   */
  public static final String TASK_VERSION_KEY = "VERSION";
  /**
   * Name of the {@link Task} class(es) for the task dynamically loaded in {@link TaskStateModel}
   */
  public static final String TASK_CLASSES_KEY = "TASK_CLASSES";
  /**
   * Name of the {@link TaskFactory} class for the task dynamically loaded in {@link TaskStateModel}
   */
  public static final String TASK_FACTORY_KEY = "TASK_FACTORY";
  /**
   * The path for dynamic task configs
   */
  public static final String DYNAMICALLY_LOADED_TASK_PATH = "/TASK_DEFINITION";
}
