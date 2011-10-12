package com.linkedin.clustermanager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.EspressoStorageMockStateModelFactory;
import com.linkedin.clustermanager.EspressoStorageMockStateModelFactory.EspressoStorageMockStateModel;
import com.linkedin.clustermanager.healthcheck.PerformanceHealthReportProvider;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.participant.statemachine.StateModel;

public class EspressoStorageMockNode extends MockNode {

	private static final Logger logger = Logger
			.getLogger(EspressoStorageMockNode.class);

	PerformanceHealthReportProvider _healthProvider;
	EspressoStorageMockStateModelFactory _stateModelFactory;

	HashSet<String>_partitions;
	
	public EspressoStorageMockNode(CMConnector cm) {
		super(cm);
		_stateModelFactory = new EspressoStorageMockStateModelFactory(0);

		StateMachineEngine<StateModel> genericStateMachineHandler = new StateMachineEngine<StateModel>(
				_stateModelFactory);
		_cmConnector
				.getManager()
				.getMessagingService()
				.registerMessageHandlerFactory(
						MessageType.STATE_TRANSITION.toString(),
						genericStateMachineHandler);
		_healthProvider = new PerformanceHealthReportProvider();
		_cmConnector.getManager().getHealthReportCollector()
				.addHealthReportProvider(_healthProvider);
		_partitions = new HashSet<String>();
		Thread partitionGetter = new Thread(new PartitionGetterThread());
		partitionGetter.start();
		logger.debug("set partition getter thread to run");
	}

	class PartitionGetterThread implements Runnable {

		@Override
		public void run() {
			while (true) {
				synchronized (_partitions) {
					//logger.debug("Building partition map");
					_partitions.clear();
					ConcurrentMap<String, StateModel> stateModelMap = _stateModelFactory
							.getStateModelMap();
					for (String s: stateModelMap.keySet()) {
						//logger.debug("adding key "+s);
						_partitions.add(s);
					}
				}
				//sleep for 60 seconds
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			}
		}
	}
	
	@Override
	public void run() {
		logger.debug("In Run");

		_healthProvider.submitRequestCount(555);
		logger.debug("Done writing stats");

		int i=0;
		while (i<1000) {
			logger.debug("printing partition map");
			synchronized (_partitions) {
				for (String partition: _partitions) {
					//logger.debug(s);
					_healthProvider.submitPartitionRequestCount(partition, i);
				}
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			i++;
		}
		logger.debug("Done!");
	}

}
