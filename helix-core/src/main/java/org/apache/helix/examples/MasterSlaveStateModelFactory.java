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

@SuppressWarnings("rawtypes")
public class MasterSlaveStateModelFactory extends StateModelFactory<StateModel> {
	int _delay;

	public MasterSlaveStateModelFactory(int delay) {
		_delay = delay;
	}

	@Override
	public StateModel createNewStateModel(String stateUnitKey) {
		MasterSlaveStateModel stateModel = new MasterSlaveStateModel();
		stateModel.setDelay(_delay);
		stateModel.setStateUnitKey(stateUnitKey);
		return stateModel;
	}

	public static class MasterSlaveStateModel extends StateModel {
		int _transDelay = 0;
		String stateUnitKey;
		
		public String getStateUnitKey() {
			return stateUnitKey;
		}

		public void setStateUnitKey(String stateUnitKey) {
			this.stateUnitKey = stateUnitKey;
		}

		public void setDelay(int delay) {
			_transDelay = delay > 0 ? delay : 0;
		}

		public void onBecomeSlaveFromOffline(Message message,
				NotificationContext context) {

			System.out.println("MasterSlaveStateModel.onBecomeSlaveFromOffline() for "+ stateUnitKey);
			sleep();
		}

		private void sleep() {
			try {
				Thread.sleep(_transDelay);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void onBecomeSlaveFromMaster(Message message,
				NotificationContext context) {
			System.out.println("MasterSlaveStateModel.onBecomeSlaveFromMaster() for "+ stateUnitKey);
			sleep();

		}

		public void onBecomeMasterFromSlave(Message message,
				NotificationContext context) {
			System.out.println("MasterSlaveStateModel.onBecomeMasterFromSlave() for "+ stateUnitKey);
			sleep();

		}

		public void onBecomeOfflineFromSlave(Message message,
				NotificationContext context) {
			System.out.println("MasterSlaveStateModel.onBecomeOfflineFromSlave() for "+ stateUnitKey);
			sleep();

		}
		
		public void onBecomeDroppedFromOffline(Message message,
        NotificationContext context) {
      System.out.println("ObBecomeDroppedFromOffline() for "+ stateUnitKey);
      sleep();

    }
	}

}
