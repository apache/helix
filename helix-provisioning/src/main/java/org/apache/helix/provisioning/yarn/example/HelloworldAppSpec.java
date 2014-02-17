package org.apache.helix.provisioning.yarn.example;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.provisioning.yarn.AppConfig;
import org.apache.helix.provisioning.yarn.ApplicationSpec;
import org.apache.helix.provisioning.yarn.TaskConfig;

public class HelloworldAppSpec implements ApplicationSpec {

  public String _appName;

  public AppConfig _appConfig;

  public List<String> _services;

  public String _appMasterPackageUri;

  public Map<String, String> _servicePackageURIMap;

  public Map<String, String> _serviceMainClassMap;

  public Map<String,Map<String,String>> _serviceConfigMap;

  public List<TaskConfig> _taskConfigs;
  @Override
  public String getAppName() {
    return _appName;
  }

  @Override
  public AppConfig getConfig() {
    return _appConfig;
  }

  @Override
  public List<String> getServices() {
    return _services;
  }

  @Override
  public URI getAppMasterPackage() {
    try {
      return new URI(_appMasterPackageUri);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  @Override
  public URI getServicePackage(String serviceName) {
    try {
      return new URI(_servicePackageURIMap.get(serviceName));
    } catch (URISyntaxException e) {
      return null;
    }
  }

  @Override
  public String getServiceMainClass(String service) {
    return _serviceMainClassMap.get(service);
  }

  @Override
  public ParticipantConfig getParticipantConfig(String serviceName, ParticipantId participantId) {
    ParticipantConfig.Builder builder = new ParticipantConfig.Builder(participantId);
    Scope<ParticipantId> scope = Scope.participant(participantId);
    UserConfig userConfig = new UserConfig(scope);
    Map<String, String> map = _serviceConfigMap.get(serviceName);
    userConfig.setSimpleFields(map);
    return builder.addTag(serviceName).userConfig(userConfig ).build();
  }

  @Override
  public List<TaskConfig> getTaskConfigs() {
    return _taskConfigs;
  }

}
