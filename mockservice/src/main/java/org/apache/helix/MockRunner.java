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
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Hello world!
 *
 */
public class MockRunner 
{
	
	private static final Logger logger = Logger.getLogger(MockRunner.class);
	
	protected static final String nodeType = "EspressoStorage";
	protected static final String ZK_ADDR = "localhost:2184";
	protected static final String INSTANCE_NAME = "localhost_1234";
	protected static final String CLUSTER_NAME = "MockCluster";
	
    public static void main( String[] args )
    {
    	//ZkClient zkClient = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
    	CMConnector cm = null;
    	try {
    		cm = new CMConnector(CLUSTER_NAME, INSTANCE_NAME, ZK_ADDR); //, zkClient);
    	}
    	catch (Exception e) {
    		logger.error("Unable to initialize CMConnector: "+e);
    		e.printStackTrace();
    		System.exit(-1);
    	}
        MockNode mock = MockNodeFactory.createMockNode(nodeType, cm);
        if (mock != null) {
        	mock.run();
        }
        else {
        	logger.error("Unknown MockNode type "+nodeType);
        	System.exit(-1);
        }
    }
}
