package com.linkedin.helix;

import com.linkedin.helix.HelixAgent;
import com.linkedin.helix.HelixAgentFactory;
import com.linkedin.helix.InstanceType;


public class CMConnector {

	HelixAgent _manager;

	public CMConnector(final String clusterName, final String instanceName, final String zkAddr) throws Exception //, final ZkClient zkClient) throws Exception
	{
		 _manager = null;
		 _manager = HelixAgentFactory
		            .getZKHelixAgent(clusterName, instanceName, InstanceType.PARTICIPANT, zkAddr); //, zkClient);
		 _manager.connect();
	}

	public HelixAgent getManager() {
		return _manager;
	}

	public void disconnect() {
		_manager.disconnect();
	}
}
