package com.linkedin.clustermanager;

//import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
//import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

//import com.linkedin.espresso.router.RoutingToken;

import com.linkedin.clustermanager.EspressoStorageMockStateModelFactory;
//import com.linkedin.clustermanager.EspressoStorageMockStateModelFactory.EspressoStorageMockStateModel;
//import com.linkedin.clustermanager.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.clustermanager.healthcheck.PerformanceHealthReportProvider;
import com.linkedin.clustermanager.healthcheck.StatHealthReportProvider;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.participant.statemachine.StateModel;

public class EspressoStorageMockNode extends MockNode {

	private static final Logger logger = Logger
			.getLogger(EspressoStorageMockNode.class);

	private final String GET_STAT_NAME = "get";
	private final String SET_STAT_NAME = "set";
	private final String COUNT_STAT_TYPE = "count";
	private final String REPORT_NAME = "ParticipantStats";
	
	StatHealthReportProvider _healthProvider;
	//PerformanceHealthReportProvider _healthProvider;
	EspressoStorageMockStateModelFactory _stateModelFactory;

	HashSet<String>_partitions;
	
	ConcurrentHashMap<String, String> _keyValueMap;
	FnvHashFunction _hashFunction;
	int _numTotalEspressoPartitions = 0;
	
	public EspressoStorageMockNode(CMConnector cm) {
		super(cm);
		_stateModelFactory = new EspressoStorageMockStateModelFactory(0);

		StateMachineEngine genericStateMachineHandler = new StateMachineEngine();
		genericStateMachineHandler.registerStateModelFactory("MasterSlave", _stateModelFactory);
		_cmConnector
				.getManager()
				.getMessagingService()
				.registerMessageHandlerFactory(
						MessageType.STATE_TRANSITION.toString(),
						genericStateMachineHandler);
        /*
		_healthProvider = new StatHealthReportProvider();
		_healthProvider.setReportName(REPORT_NAME);
       */

		_healthProvider = new StatHealthReportProvider();
		//_healthProvider.setReportName(REPORT_NAME);

		_cmConnector.getManager().getHealthReportCollector()
				.addHealthReportProvider(_healthProvider);
		_partitions = new HashSet<String>();
		_keyValueMap = new ConcurrentHashMap<String, String>();
		_hashFunction = new FnvHashFunction();
		
		//start thread to keep checking what partitions this node owns
		//Thread partitionGetter = new Thread(new PartitionGetterThread());
		//partitionGetter.start();
		//logger.debug("set partition getter thread to run");
	}

	public String formStatName(String dbName, String partitionName, String metricName)
	{
		String statName;
		statName = "db"+dbName+".partition"+partitionName+"."+metricName;
		return statName;
		
	}
	
	public String doGet(String dbId, String key) {
		String partition = getPartitionName(dbId, getKeyPartition(dbId, key));
		if (!isPartitionOwnedByNode(partition)) {
			logger.error("Key "+key+" hashed to partition "+partition +" but this node does not own it.");
			return null;
		}

		//_healthProvider.submitIncrementPartitionRequestCount(partition);
		//_healthProvider.incrementPartitionStat(GET_STAT_NAME, partition);
		_healthProvider.incrementStat(formStatName(dbId, partition, "getCount"), String.valueOf(System.currentTimeMillis()));
		return _keyValueMap.get(key);
	}
	
	public void doPut(String dbId, String key, String value) {
		String partition = getPartitionName(dbId, getKeyPartition(dbId, key));
		if (!isPartitionOwnedByNode(partition)) {
			logger.error("Key "+key+" hashed to partition "+partition +" but this node does not own it.");
			return;
		}
		
		//_healthProvider.submitIncrementPartitionRequestCount(partition);
		//_healthProvider.incrementPartitionStat(SET_STAT_NAME, partition);
		//_healthProvider.incrementStat(SET_STAT_NAME, COUNT_STAT_TYPE, 
		//		dbId, partition, "FIXMENODENAME", String.valueOf(System.currentTimeMillis()));
		_healthProvider.incrementStat(formStatName(dbId, partition, "putCount"), String.valueOf(System.currentTimeMillis()));
		
		_keyValueMap.put(key, value);
	}
	
	private String getPartitionName(String databaseName, int partitionNum) {
		return databaseName+"_"+partitionNum;
	}
	
	private boolean isPartitionOwnedByNode(String partitionName) {
		Map<String, StateModel> stateModelMap = _stateModelFactory
				.getStateModelMap();
		logger.debug("state model map size: "+stateModelMap.size());
		
		return (stateModelMap.keySet().contains(partitionName));
	}
	
	private int getKeyPartition(String dbName, String key) {
		int numPartitions = getNumPartitions(dbName);
		logger.debug("numPartitions: "+numPartitions);
		int part = Math.abs((int)_hashFunction.hash(key.getBytes(), numPartitions));
		logger.debug("part: "+part);
		return part;
	}
	
	private int getNumPartitions(String dbName) {
		logger.debug("dbName: "+dbName);
		ZNRecord rec = _cmConnector.getManager().getDataAccessor().getProperty(PropertyType.IDEALSTATES, dbName);
		if (rec == null) {
			logger.debug("rec is null");
		}
		IdealState state = new IdealState(rec);
		return state.getNumPartitions();
	}
	
	class PartitionGetterThread implements Runnable {

		@Override
		public void run() {
			while (true) {
				synchronized (_partitions) {
					//logger.debug("Building partition map");
					_partitions.clear();
					Map<String, StateModel> stateModelMap = _stateModelFactory
							.getStateModelMap();
					for (String s: stateModelMap.keySet()) {
						logger.debug("adding key "+s);
						_partitions.add(s);
					}
				}
				//sleep for 60 seconds
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			}
		}
	}
	
	@Override
	public void run() {
		
	}


	

}
