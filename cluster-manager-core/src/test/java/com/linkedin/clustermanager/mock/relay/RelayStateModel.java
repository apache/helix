package com.linkedin.clustermanager.mock.relay;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.StateModel;

public class RelayStateModel extends StateModel
{
  private RelayAdapter relay;
  private static Logger logger = Logger.getLogger(RelayStateModel.class);

  public RelayStateModel(String stateUnitKey, RelayAdapter relayAdapter)
  {
    // relayConsumersMap = new HashMap<Integer,RelayConsumer>();
    relay = relayAdapter;
  }

  void checkDebug(Message task) throws Exception
  {
    // For debugging purposes
    if ((Boolean) task.getDebug() == true)
    {
      throw new Exception("Exception for debug");
    }
  }

  // @transition(to='to',from='from',blah blah..)
  public void onBecomeSlaveFromOffline(Message task, NotificationContext context)
      throws Exception
  {

    checkDebug(task);
    logger.info("Became slave for partition " + task.getStateUnitKey());
  }

  public void onBecomeSlaveFromMaster(Message task, NotificationContext context)
      throws Exception
  {
    checkDebug(task);
    logger.info("Became slave for partition " + task.getStateUnitKey());
  }

  public void onBecomeMasterFromSlave(Message task, NotificationContext context)
      throws Exception
  {
    checkDebug(task);
    logger.info("Became master for partition " + task.getStateUnitKey());
  }

  public void onBecomeOfflineFromSlave(Message task, NotificationContext context)
      throws Exception
  {
    checkDebug(task);
    logger.info("Became offline for partition " + task.getStateUnitKey());
  }
}
