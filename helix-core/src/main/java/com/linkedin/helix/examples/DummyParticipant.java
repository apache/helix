package com.linkedin.helix.examples;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;

public class DummyParticipant
{
  // dummy master-slave state model
  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE", "ERROR" })
  public static class DummyMSStateModel extends StateModel
  {
    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
    {
      String partitionName = message.getPartitionName();
      System.out.println(partitionName + " becomes SLAVE from OFFLINE");
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
    {
      String partitionName = message.getPartitionName();
      System.out.println(partitionName + " becomes MASTER from SLAVE");
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
    {
      String partitionName = message.getPartitionName();
      System.out.println(partitionName + " becomes SLAVE from MASTER");
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context)
    {
      String partitionName = message.getPartitionName();
      System.out.println(partitionName + " becomes OFFLINE from SLAVE");
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
        throws InterruptedException
    {
      String partitionName = message.getPartitionName();
      System.out.println(partitionName + " becomes DROPPED from OFFLINE");
    }

    @Transition(to = "OFFLINE", from = "ERROR")
    public void onBecomeOfflineFromError(Message message, NotificationContext context)
        throws InterruptedException
    {
      String partitionName = message.getPartitionName();
      System.out.println(partitionName + " becomes OFFLINE from ERROR");
    }

    @Override
    public void reset()
    {
      System.out.println("Default MockMSStateModel.reset() invoked");
    }
  }

  // dummy master slave state model factory
  public static class DummyMSModelFactory extends StateModelFactory<DummyMSStateModel>
  {
    @Override
    public DummyMSStateModel createNewStateModel(String partitionName)
    {
      DummyMSStateModel model = new DummyMSStateModel();
      return model;
    }
  }

  public static void main(String[] args)
  {
    if (args.length < 3)
    {
      System.err.println("USAGE: DummyParticipant zkAddress clusterName instanceName");
      System.exit(1);
    }

    String zkAddr = args[0];
    String clusterName = args[1];
    String instanceName = args[2];

    HelixManager manager = null;
    try
    {
      manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName,
          InstanceType.PARTICIPANT, zkAddr);

      StateMachineEngine stateMach = manager.getStateMachineEngine();
      DummyMSModelFactory msModelFactory = new DummyMSModelFactory();
      stateMach.registerStateModelFactory("MasterSlave", msModelFactory);

      manager.connect();

      Thread.currentThread().join();
    } catch (InterruptedException e)
    {
      String msg = "Dummy participant: " + instanceName + ", " + Thread.currentThread().getName()
          + " is interrupted";
      System.err.println(msg);
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally
    {
      if (manager != null)
      {
        manager.disconnect();
      }
    }
  }
}
