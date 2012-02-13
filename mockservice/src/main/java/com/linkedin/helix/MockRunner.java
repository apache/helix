package com.linkedin.helix;

import org.apache.log4j.Logger;

import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;

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
