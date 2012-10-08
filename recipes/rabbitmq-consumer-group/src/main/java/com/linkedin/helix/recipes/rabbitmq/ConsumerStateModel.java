package com.linkedin.helix.recipes.rabbitmq;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;

@StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE", "ERROR" })
public class ConsumerStateModel extends StateModel
{
  private static Logger LOG = Logger.getLogger(ConsumerStateModel.class);

  private final String _consumerId;
  private final String _partition;

  private final String _mqServer;
  private ConsumerThread _thread = null;

  public ConsumerStateModel(String consumerId, String partition, String mqServer)
  {
    _partition = partition;
    _consumerId = consumerId;
    _mqServer = mqServer;
  }

  @Transition(to = "SLAVE", from = "OFFLINE")
  public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
  {
//    String partition = message.getPartitionName();
//    String consumerId = message.getTgtName();
    LOG.debug(_consumerId + " becomes SLAVE from OFFLINE for " + _partition);
  }

  @Transition(to = "MASTER", from = "SLAVE")
  public void onBecomeMasterFromSlave(Message message, NotificationContext context)
  {
//    String partition = message.getPartitionName();
//    String consumerId = message.getTgtName();
    LOG.debug(_consumerId + " becomes MASTER from SLAVE for " + _partition);

    if (_thread == null)
    {
      LOG.debug("Starting ConsumerThread for " + _partition + "...");
      _thread = new ConsumerThread(_partition, _mqServer, _consumerId);
      _thread.start();
      LOG.debug("Starting ConsumerThread for " + _partition + " done");

    }
  }

  @Transition(to = "SLAVE", from = "MASTER")
  public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
      throws InterruptedException
  {
//    String partition = message.getPartitionName();
//    String consumerId = message.getTgtName();
    LOG.debug(_consumerId + " becomes SLAVE from MASTER for " + _partition);

    if (_thread != null)
    {
      LOG.debug("Stopping " + _consumerId + " for " + _partition + "...");
//      System.out.println("Stopping " + _consumerId + " for " + _partition + "...");

      _thread.interrupt();
      _thread.join(2000);
      _thread = null;
      LOG.debug("Stopping " +  _consumerId + " for " + _partition + " done");

    }
  }

  @Transition(to = "OFFLINE", from = "SLAVE")
  public void onBecomeOfflineFromSlave(Message message, NotificationContext context)
  {
//    String partition = message.getPartitionName();
//    String consumerId = message.getTgtName();
    LOG.debug(_consumerId + " becomes OFFLINE from SLAVE for " + _partition);
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
  {
//    String partition = message.getPartitionName();
//    String consumerId = message.getTgtName();
    LOG.debug(_consumerId + " becomes DROPPED from OFFLINE for " + _partition);
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context)
  {
//    String partition = message.getPartitionName();
//    String consumerId = message.getTgtName();
    LOG.debug(_consumerId + " becomes OFFLINE from ERROR for " + _partition);
  }

  @Override
  public void reset()
  {
    LOG.warn("Default reset() invoked");
    
    if (_thread != null)
    {
      LOG.debug("Stopping " + _consumerId + " for " + _partition + "...");
//      System.out.println("Stopping " + _consumerId + " for " + _partition + "...");

      _thread.interrupt();
      try
      {
        _thread.join(2000);
      } catch (InterruptedException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      _thread = null;
      LOG.debug("Stopping " +  _consumerId + " for " + _partition + " done");

    }

  }
}
