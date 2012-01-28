package com.linkedin.helix.examples;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;

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
		
		public void onBecomeDroppedFromOffline(Message message,
        NotificationContext context) {
      System.out.println("OnlineOfflineStateModel.onBecomeDroppedFromOffline()");
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
