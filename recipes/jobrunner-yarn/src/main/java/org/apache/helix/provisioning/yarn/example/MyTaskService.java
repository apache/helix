package org.apache.helix.provisioning.yarn.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixManager;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.HelixConnectionAdaptor;
import org.apache.helix.participant.AbstractParticipantService;
import org.apache.helix.provisioning.ServiceConfig;
import org.apache.helix.provisioning.participant.StatelessParticipantService;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.log4j.Logger;

/**
 * A simple "service" for task callback registration.
 */
public class MyTaskService extends StatelessParticipantService {

  private static Logger LOG = Logger.getLogger(AbstractParticipantService.class);

  static String SERVICE_NAME = "JobRunner";

  public MyTaskService(HelixConnection connection, ClusterId clusterId,
      ParticipantId participantId) {
    super(connection, clusterId, participantId, SERVICE_NAME);
  }

  @Override
  protected void init(ServiceConfig serviceConfig) {
    LOG.info("Initialized service with config " + serviceConfig);

    // Register for callbacks for tasks
    HelixManager manager = new HelixConnectionAdaptor(getParticipant());
    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("RunTask", new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new MyTask(context);
      }
    });
    getParticipant().getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.from("Task"), new TaskStateModelFactory(manager, taskFactoryReg));
  }

  @Override
  protected void goOnline() {
    LOG.info("JobRunner service is told to go online");
  }

  @Override
  protected void goOffine() {
    LOG.info("JobRunner service is told to go offline");
  }

}
