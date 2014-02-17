package org.apache.helix.provisioning.yarn;

import java.net.URI;
import java.util.List;

import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.id.ParticipantId;

public interface ApplicationSpec {
  /**
   * Returns the name of the application
   * @return
   */
  String getAppName();

  AppConfig getConfig();

  List<String> getServices();

  URI getAppMasterPackage();
  
  URI getServicePackage(String serviceName);
  
  String getServiceMainClass(String service);

  ParticipantConfig getParticipantConfig(String serviceName, ParticipantId participantId);

  List<TaskConfig> getTaskConfigs();

}
