package org.apache.helix;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
//import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
//import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;

import org.apache.helix.EspressoStorageMockStateModelFactory;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.healthcheck.PerformanceHealthReportProvider;
import org.apache.helix.healthcheck.StatHealthReportProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.log4j.Logger;

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

//		StateMachineEngine genericStateMachineHandler = new StateMachineEngine();
		StateMachineEngine stateMach = _cmConnector.getManager().getStateMachineEngine();
		stateMach.registerStateModelFactory("MasterSlave", _stateModelFactory);
//		_cmConnector
//				.getManager()
//				.getMessagingService()
//				.registerMessageHandlerFactory(
//						MessageType.STATE_TRANSITION.toString(),
//						genericStateMachineHandler);
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
		Set<String> stateModels = _stateModelFactory.getPartitionSet();
		logger.debug("state model map size: "+stateModels.size());

		return (stateModels.contains(partitionName));
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
		HelixDataAccessor helixDataAccessor = _cmConnector.getManager().getHelixDataAccessor();
		Builder keyBuilder = helixDataAccessor.keyBuilder();
    ZNRecord rec = helixDataAccessor.getProperty(keyBuilder.idealStates(dbName)).getRecord();
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
					Set<String> stateModels = _stateModelFactory.getPartitionSet();
					for (String s: stateModels) {
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
