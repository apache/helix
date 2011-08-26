package com.linkedin.clustermanager.participant.statemachine;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;

public abstract class StateModel
{
	public static final String DEFAULT_INITIAL_STATE = "OFFLINE";
	Logger logger = Logger.getLogger(StateModel.class);
	// TODO Get default state from implementation or from state model annotation
	private String _currentState = DEFAULT_INITIAL_STATE;

	public String getCurrentState()
	{
		return _currentState;
	}

	// @transition(from='from', to='to')
	public void defaultTransitionHandler()
	{
		logger
		    .error("Default default handler. The idea is to invoke this if no transition method is found. Yet to be implemented");
	}

	public boolean updateState(String newState)
	{
		_currentState = newState;
		return true;
	}

	/**
	 * 
	 * @param message
	 * @param context
	 * @param error
	 */
	// todo:enforce subclass to write this
	public void rollbackOnError(Message message, NotificationContext context,
	    StateTransitionError error)
	{

		logger.error("Default rollback method invoked on error. Error Code:"
		    + error.getCode());

	}

	/**
	 * This metho
	 */
	public void reset()
	{
		logger
		    .error("Default reset method invoked. Either because the process longer own this resource or session timedout");
	}

}
