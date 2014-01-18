package org.apache.helix.provisioning.yarn;

import java.util.List;

public interface ApplicationSpec {
	public String getAppName();
	public int getMinContainers();
	public int getMaxContainers();
	public AppConfig getConfig();
	public List<ServiceConfig> getServices();
}
