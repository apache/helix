package org.apache.helix.provisioning;

import java.net.URI;
import java.util.List;



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

  ServiceConfig getServiceConfig(String serviceName);

  List<TaskConfig> getTaskConfigs();

}
