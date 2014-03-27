/*
 * $Id$
 */
package org.apache.helix.task;

import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.participant.statemachine.StateModelFactory;

/**
 * Factory class for {@link TaskStateModel}.
 */
public class TaskStateModelFactory extends StateModelFactory<TaskStateModel> {
  private final HelixManager _manager;
  private final Map<String, TaskFactory> _taskFactoryRegistry;

  public TaskStateModelFactory(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry) {
    _manager = manager;
    _taskFactoryRegistry = taskFactoryRegistry;
  }

  @Override
  public TaskStateModel createNewStateModel(String partitionName) {
    return new TaskStateModel(_manager, _taskFactoryRegistry);
  }
}
