package com.linkedin.clustermanager;


public class CMConnector {

	ClusterManager _manager;

	public CMConnector(final String clusterName, final String instanceName, final String zkAddr) throws Exception //, final ZkClient zkClient) throws Exception
	{
		 _manager = null;
		 _manager = ClusterManagerFactory
		            .getZKClusterManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkAddr); //, zkClient);
		 _manager.connect();
	}

	public ClusterManager getManager() {
		return _manager;
	}

	public void disconnect() {
		_manager.disconnect();
	}
}
