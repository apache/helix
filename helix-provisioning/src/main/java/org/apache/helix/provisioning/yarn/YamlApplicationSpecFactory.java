package org.apache.helix.provisioning.yarn;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

class DefaultApplicationSpec implements ApplicationSpec {
	public String appName;
	public Integer minContainers;
	public Integer maxContainers;
	
	public AppConfig appConfig;

	public List<String> services;
	public Map<String, ServiceConfig> serviceConfigMap;

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
	public List<String> getServices() {
		return services;
	}

	@Override
	public ServiceConfig getServiceConfig(String name) {
		return serviceConfigMap.get(name);
	}
}

public class YamlApplicationSpecFactory {
	ApplicationSpec fromYaml(InputStream input) {
	    Yaml yaml = new Yaml();
	    return yaml.loadAs(input, DefaultApplicationSpec.class);
	}
}
