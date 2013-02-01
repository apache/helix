package org.apache.helix;

import java.util.List;

import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;

// ZkHelixManager used for test only. expose more class members
public class ZkHelixTestManager extends ZKHelixManager {

	public ZkHelixTestManager(String clusterName, String instanceName, InstanceType instanceType,
	        String zkConnectString) throws Exception {
		super(clusterName, instanceName, instanceType, zkConnectString);
		// TODO Auto-generated constructor stub
	}

	public ZkClient getZkClient() {
		return _zkClient;
	}

	public List<CallbackHandler> getHandlers() {
		return _handlers;
	}
}
