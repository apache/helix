package com.linkedin.clustermanager.examples;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;

public class OnlineOfflineStateModelFactory extends
		StateModelFactory<StateModel> {
	int _delay;

	public OnlineOfflineStateModelFactory(int delay) {
		_delay = delay;
	}

	@Override
	public StateModel createNewStateModel(String stateUnitKey) {
		OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel();
		stateModel.setDelay(_delay);
		return stateModel;
	}

	public static class OnlineOfflineStateModel extends StateModel {
		int _transDelay = 0;

		public void setDelay(int delay) {
			_transDelay = delay > 0 ? delay : 0;
		}

		public void onBecomeOnlineFromOffline(Message message,
				NotificationContext context) {
			System.out
					.println("OnlineOfflineStateModel.onBecomeOnlineFromOffline()");
			sleep();
		}

		public void onBecomeOfflineFromOnline(Message message,
				NotificationContext context) {
			System.out
					.println("OnlineOfflineStateModel.onBecomeOfflineFromOnline()");
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
