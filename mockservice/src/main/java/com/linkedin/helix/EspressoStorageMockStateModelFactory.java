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
package com.linkedin.helix;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;

@SuppressWarnings("rawtypes")
public class EspressoStorageMockStateModelFactory extends StateModelFactory<StateModel> {
	int _delay;

	public EspressoStorageMockStateModelFactory(int delay) {
		_delay = delay;
	}

	@Override
	public StateModel createNewStateModel(String stateUnitKey) {
		EspressoStorageMockStateModel stateModel = new EspressoStorageMockStateModel();
		stateModel.setDelay(_delay);
		stateModel.setStateUnitKey(stateUnitKey);
		return stateModel;
	}

	public static class EspressoStorageMockStateModel extends StateModel {
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

			System.out.println("EspressoStorageMockStateModel.onBecomeSlaveFromOffline() for "+ stateUnitKey);
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
			System.out.println("EspressoStorageMockStateModel.onBecomeSlaveFromMaster() for "+ stateUnitKey);
			sleep();

		}

		public void onBecomeMasterFromSlave(Message message,
				NotificationContext context) {
			System.out.println("EspressoStorageMockStateModel.onBecomeMasterFromSlave() for "+ stateUnitKey);
			sleep();

		}

		public void onBecomeOfflineFromSlave(Message message,
				NotificationContext context) {
			System.out.println("EspressoStorageMockStateModel.onBecomeOfflineFromSlave() for "+ stateUnitKey);
			sleep();

		}
		
		public void onBecomeDroppedFromOffline(Message message,
        NotificationContext context) {
      System.out.println("ObBecomeDroppedFromOffline() for "+ stateUnitKey);
      sleep();

    }
	}

}
