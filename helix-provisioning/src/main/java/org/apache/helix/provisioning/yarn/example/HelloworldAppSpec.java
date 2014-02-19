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

  private String _appName;

  private AppConfig _appConfig;

  private List<String> _services;

  private String _appMasterPackageUri;
  
  private Map<String, String> _servicePackageURIMap;

  private Map<String, String> _serviceMainClassMap;

  private Map<String,Map<String,String>> _serviceConfigMap;

  private List<TaskConfig> _taskConfigs;

  public AppConfig getAppConfig() {
    return _appConfig;
  }

  public void setAppConfig(AppConfig appConfig) {
    _appConfig = appConfig;
  }

  public String getAppMasterPackageUri() {
    return _appMasterPackageUri;
  }

  public void setAppMasterPackageUri(String appMasterPackageUri) {
    _appMasterPackageUri = appMasterPackageUri;
  }

  public Map<String, String> getServicePackageURIMap() {
    return _servicePackageURIMap;
  }

  public void setServicePackageURIMap(Map<String, String> servicePackageURIMap) {
    _servicePackageURIMap = servicePackageURIMap;
  }

  public Map<String, String> getServiceMainClassMap() {
    return _serviceMainClassMap;
  }

  public void setServiceMainClassMap(Map<String, String> serviceMainClassMap) {
    _serviceMainClassMap = serviceMainClassMap;
  }

  public Map<String, Map<String, String>> getServiceConfigMap() {
    return _serviceConfigMap;
  }

  public void setServiceConfigMap(Map<String, Map<String, String>> serviceConfigMap) {
    _serviceConfigMap = serviceConfigMap;
  }

  public void setAppName(String appName) {
    _appName = appName;
  }

  public void setServices(List<String> services) {
    _services = services;
  }

  public void setTaskConfigs(List<TaskConfig> taskConfigs) {
    _taskConfigs = taskConfigs;
  }

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
