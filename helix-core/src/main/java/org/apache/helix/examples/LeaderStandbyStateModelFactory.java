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
package org.apache.helix.examples;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class LeaderStandbyStateModelFactory extends
		StateModelFactory<StateModel> {
	int _delay;

	public LeaderStandbyStateModelFactory(int delay) {
		_delay = delay;
	}

	@Override
	public StateModel createNewStateModel(String stateUnitKey) {
		LeaderStandbyStateModel stateModel = new LeaderStandbyStateModel();
		stateModel.setDelay(_delay);
		return stateModel;
	}

	public static class LeaderStandbyStateModel extends StateModel {
		int _transDelay = 0;

		public void setDelay(int delay) {
			_transDelay = delay > 0 ? delay : 0;
		}

		public void onBecomeLeaderFromStandby(Message message,
				NotificationContext context) {
			System.out
					.println("LeaderStandbyStateModel.onBecomeLeaderFromStandby()");
			sleep();
		}

		public void onBecomeStandbyFromLeader(Message message,
				NotificationContext context) {
			System.out
					.println("LeaderStandbyStateModel.onBecomeStandbyFromLeader()");
			sleep();
		}

		private void sleep() {
			try {
				Thread.sleep(_transDelay);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}