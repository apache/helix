package org.apache.helix.provisioning.yarn;

import java.io.InputStream;
import java.util.List;

import org.yaml.snakeyaml.Yaml;

class DefaultApplicationSpec implements ApplicationSpec {
	public String appName;
	public Integer minContainers;
	public Integer maxContainers;
	
	public AppConfig appConfig;

	public List<ServiceConfig> serviceConfig;

	@Override
	public String getAppName() {
		return appName;
	}

	@Override
	public int getMinContainers() {
		return minContainers;
	}

	@Override
	public int getMaxContainers() {
		return maxContainers;
	}

	@Override
	public AppConfig getConfig() {
		return appConfig;
	}

	@Override
	public List<ServiceConfig> getServices() {
		return serviceConfig;
	}
	
}

public class YamlApplicationSpecFactory {
	ApplicationSpec fromYaml(InputStream input) {
	    Yaml yaml = new Yaml();
	    return yaml.loadAs(input, DefaultApplicationSpec.class);
	}
}
