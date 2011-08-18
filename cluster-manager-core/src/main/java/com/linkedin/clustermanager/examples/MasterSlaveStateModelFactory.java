package com.linkedin.clustermanager.examples;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;

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
		return stateModel;
	}

	public static class MasterSlaveStateModel extends StateModel {
		int _transDelay = 0;

		public void setDelay(int delay) {
			_transDelay = delay > 0 ? delay : 0;
		}

		public void onBecomeSlaveFromOffline(Message message,
				NotificationContext context) {

			System.out.println("DummyStateModel.onBecomeSlaveFromOffline()");
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
			System.out.println("DummyStateModel.onBecomeSlaveFromMaster()");
			sleep();

		}

		public void onBecomeMasterFromSlave(Message message,
				NotificationContext context) {
			System.out.println("DummyStateModel.onBecomeMasterFromSlave()");
			sleep();

		}

		public void onBecomeOfflineFromSlave(Message message,
				NotificationContext context) {
			System.out.println("DummyStateModel.onBecomeOfflineFromSlave()");
			sleep();

		}
	}

}
