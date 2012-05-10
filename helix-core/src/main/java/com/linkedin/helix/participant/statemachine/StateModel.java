/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.participant.statemachine;

import org.apache.log4j.Logger;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public abstract class StateModel
{
	public static final String DEFAULT_INITIAL_STATE = "OFFLINE";
	Logger logger = Logger.getLogger(StateModel.class);
	// TODO Get default state from implementation or from state model annotation
	protected String _currentState = DEFAULT_INITIAL_STATE;

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
	 * This method
	 */
	public void reset()
	{
    logger.warn("Default reset method invoked. Either because the process longer own this resource or session timedout");
	}

}
