/*
 * $Id$
 */
package org.apache.helix.task.beans;

import java.util.List;

/**
 * Bean class used for parsing workflow definitions from YAML.
 */
public class WorkflowBean {
  public String name;
  public String expiry;
  public List<TaskBean> tasks;
}
