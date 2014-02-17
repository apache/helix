package org.apache.helix.provisioning.yarn;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;

/**
 * Convenient method to pass information to containers
 * The methods simply sets up environment variables
 */
public class AppMasterConfig {

  private Map<String, String> _envs;

  public enum AppEnvironment {
    APP_MASTER_PKG,
    APP_SPEC_FILE,
    APP_NAME,
    APP_ID;
  }

  public AppMasterConfig() {
    _envs = new HashMap<String, String>(System.getenv());
  }

  public void setAppId(int id) {
    _envs.put(AppEnvironment.APP_ID.toString(), "" + id);
  }

  public String getAppName() {
    return _envs.get(AppEnvironment.APP_NAME.toString());
  }

  public int getAppId() {
    return Integer.parseInt(_envs.get(AppEnvironment.APP_ID.toString()));
  }

  public String getClassPath(String serviceName) {
    return _envs.get(serviceName + ".classPath");
  }

  public String getMainClass(String serviceName) {
    return _envs.get(serviceName + ".mainClass");
  }

  public String getZKAddress() {
    return _envs.get(Environment.NM_HOST.name()) + ":2181";
  }

  public String getContainerId() {
    return _envs.get(Environment.CONTAINER_ID.name());
  }

  public Map<String, String> getEnv() {
    return _envs;
  }

  public void setAppName(String appName) {
    _envs.put(AppEnvironment.APP_NAME.toString(), appName);

  }

  public void setClasspath(String serviceName, String classpath) {
    _envs.put(serviceName + ".classpath", classpath);
  }

  public String getApplicationSpecConfigFile() {
    // TODO Auto-generated method stub
    return null;
  }

  public String getApplicationSpecProvider() {
    // TODO Auto-generated method stub
    return null;
  }
}
