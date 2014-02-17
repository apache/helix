package org.apache.helix.provisioning.yarn;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.id.ParticipantId;
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
	public AppConfig getConfig() {
		return appConfig;
	}

	@Override
	public List<String> getServices() {
		return services;
	}

  @Override
  public URI getServicePackage(String serviceName) {
    return null;
  }

  @Override
  public ParticipantConfig getParticipantConfig(String serviceName, ParticipantId participantId) {
    return null;
  }

  @Override
  public List<TaskConfig> getTaskConfigs() {
    return null;
  }

  @Override
  public URI getAppMasterPackage() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getServiceMainClass(String service) {
    // TODO Auto-generated method stub
    return null;
  }
}

public class YamlApplicationSpecFactory {
	ApplicationSpec fromYaml(InputStream input) {
	    Yaml yaml = new Yaml();
	    return yaml.loadAs(input, DefaultApplicationSpec.class);
	}
}
