package com.linkedin.clustermanager;

import org.apache.log4j.Logger;

public abstract class MockNode {
	CMConnector _cmConnector;
	
	public MockNode(CMConnector cm) {
		_cmConnector = cm;
	}

	public abstract void run();
	
	public void disconnect() {
		_cmConnector.disconnect();
	}
}
