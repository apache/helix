/*
 * $Id$
 */
package org.apache.helix.task;

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
}
