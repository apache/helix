package com.linkedin.clustermanager.participant;

import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.CMTaskExecutor;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class StateMachineEngine<T extends StateModel> implements MessageListener
{
  private static Logger logger = Logger.getLogger(StateMachineEngine.class);
  private final StateModelFactory<T> _stateModelFactory;
  private final CMTaskExecutor _taskExecutor;
  StatusUpdateUtil _statusUpdateUtil;

  public StateModelFactory<T> getStateModelFactory()
  {
    return _stateModelFactory;
  }

  public StateMachineEngine(StateModelFactory<T> factory)
  {
    this._stateModelFactory = factory;
    _taskExecutor = new CMTaskExecutor();
    _statusUpdateUtil = new StatusUpdateUtil();
  }

  @Override
  public void onMessage(String instanceName, List<ZNRecord> messages,
      NotificationContext changeContext)
  {
    ClusterManager manager = changeContext.getManager();
    ClusterDataAccessor client = manager.getDataAccessor();
    // check the taskId, see if there is already a task started
    // if no task
    // lookup statetabel for the to and from and invoke the corresponding
    // method on the statemodel
    // after completion/error update the zk state
    if(messages ==null || messages.size()==0){
        logger.info("No Messages to process");
        return;
    }
    for (ZNRecord temp : messages)
    {
      // TODO temp fix since current version of jackson does not support
      // polymorphic conversion
      Message record = null;
      if (!(temp instanceof Message))
      {
        record = new Message(temp);
      } else
      {
        record = (Message) temp;
      }
      if (record instanceof Message)
      {
        Message message = (Message) record;
        // another hack for jackson problem
        if (message.getId() == null)
        {
          message.setId(message.getMsgId());
        }
        String sessionId = manager.getSessionId();
        String tgtSessionId = ((Message) message).getTgtSessionId();
        if (sessionId.equals(tgtSessionId))
        {
          if ("new".equals(message.getMsgState()))
          {
            System.err.println("ID=" + message.getId());
            String stateUnitKey = message.getStateUnitKey();
//            StateModel stateModel;
            T stateModel = _stateModelFactory.getStateModel(stateUnitKey);
            if (stateModel == null)
            {
              stateModel = _stateModelFactory.createNewStateModel(stateUnitKey);
              _stateModelFactory.addStateModel(stateUnitKey, stateModel);
            }
            // update msgState to read
            message.setMsgState("read");
            
            _statusUpdateUtil.logInfo(
                message, 
                StateMachineEngine.class, 
                "Message get read",
                client
                );
            
            client.updateInstanceProperty(instanceName, InstancePropertyType.MESSAGES, message.getId(), message);
            _taskExecutor.executeTask(message, stateModel, changeContext);
            
          } else
          {
            logger.trace("Message already processed" + message.getMsgId());
            _statusUpdateUtil.logInfo(
                message, 
                StateMachineEngine.class, 
                "Message already read",
                client
                );
          }

        } else
        {
          String warningMessage = "Session Id does not match.  current session id  Expected: " + sessionId
          + " sessionId from Message: " + tgtSessionId;
          logger.warn(warningMessage);
          
          _statusUpdateUtil.logWarning(
              message, 
              StateMachineEngine.class, 
              warningMessage,
              client
              );
        }
      } else
      {
        String warningMessage = "Invalid message format.Must be of type Message:" + record.getClass();
        logger.warn(warningMessage);
        
        _statusUpdateUtil.logWarning(
            record, 
            StateMachineEngine.class, 
            warningMessage,
            client
            );
      }
    }

  }

}
