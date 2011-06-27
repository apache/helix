package com.linkedin.clustermanager.statemachine;


import org.apache.log4j.Logger;

import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.model.Message;

public abstract class StateModel
{
  Logger logger = Logger.getLogger(StateModel.class);
	// TODO Get default state from implementation or from state model annotation
  private String _currentState = "OFFLINE";
  
  
  public String getCurrentState()
  {
    return _currentState;
  }

  // @transition(from='from', to='to')
  public void defaultTransitionHandler()
  {

  }

  public boolean updateState(String newState)
  {
    _currentState = newState;
    return true;
  }
  //todo:enforce subclass to write this
  public void rollbackOnError(Message message,NotificationContext context,StateTransitionError error){
	  
	  logger.error("Default rollback method invoked on error. Error Code:"+ error.getCode());
	  
  }

}
