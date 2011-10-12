package com.linkedin.clustermanager;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;

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
