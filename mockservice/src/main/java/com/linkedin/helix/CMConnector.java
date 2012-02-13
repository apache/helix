package com.linkedin.helix;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;


public class CMConnector {

	HelixManager _manager;

	public CMConnector(final String clusterName, final String instanceName, final String zkAddr) throws Exception //, final ZkClient zkClient) throws Exception
	{
		 _manager = null;
		 _manager = HelixManagerFactory
		            .getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkAddr); //, zkClient);
		 _manager.connect();
	}

	public HelixManager getManager() {
		return _manager;
	}

	public void disconnect() {
		_manager.disconnect();
	}
}
