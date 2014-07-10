package org.apache.helix.provisioning.yarn;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.log4j.Logger;

/**
 * Convenient method to pass information to containers
 * The methods simply sets up environment variables
 */
public class AppMasterConfig {
  private static Logger LOG = Logger.getLogger(AppMasterConfig.class);
  private Map<String, String> _envs;

  public enum AppEnvironment {
    APP_MASTER_PKG("APP_MASTER_PKG"),
    APP_SPEC_FILE("APP_SPEC_FILE"),
    APP_NAME("APP_NAME"),
    APP_ID("APP_ID"),
    APP_SPEC_FACTORY("APP_SPEC_FACTORY"),
    TASK_CONFIG_FILE("TASK_CONFIG_FILE");
    String _name;

    private AppEnvironment(String name) {
      _name = name;
    }

    public String toString() {
      return _name;
    }
  }

  public AppMasterConfig() {
    _envs = new HashMap<String, String>();
  }

  private String get(String key) {
    String value = (_envs.containsKey(key)) ? _envs.get(key) : System.getenv().get(key);
    LOG.info("Returning value:" + value + " for key:'" + key + "'");

    return value;
  }

  public void setAppId(int id) {
    _envs.put(AppEnvironment.APP_ID.toString(), "" + id);
  }

  public String getAppName() {
    return get(AppEnvironment.APP_NAME.toString());
  }

  public int getAppId() {
    return Integer.parseInt(get(AppEnvironment.APP_ID.toString()));
  }

  public String getClassPath(String serviceName) {
    return get(serviceName + "_classpath");
  }

  public String getMainClass(String serviceName) {
    return get(serviceName + "_mainClass");
  }

  public String getZKAddress() {
    return get(Environment.NM_HOST.name()) + ":2181";
  }

  public String getContainerId() {
    return get(Environment.CONTAINER_ID.name());
  }

  public Map<String, String> getEnv() {
    return _envs;
  }

  public void setAppName(String appName) {
    _envs.put(AppEnvironment.APP_NAME.toString(), appName);

  }

  public void setClasspath(String serviceName, String classpath) {
    _envs.put(serviceName + "_classpath", classpath);
  }

  public void setTaskConfigFile(String configName, String path) {
    _envs.put(AppEnvironment.TASK_CONFIG_FILE.toString() + "_" + configName, path);
  }

  public String getTaskConfigFile(String configName) {
    return get(AppEnvironment.TASK_CONFIG_FILE.toString() + "_" + configName);
  }

  public String getApplicationSpecConfigFile() {
    return get(AppEnvironment.APP_SPEC_FILE.toString());
  }

  public String getApplicationSpecFactory() {
    return get(AppEnvironment.APP_SPEC_FACTORY.toString());
  }

  public void setApplicationSpecFactory(String className) {
    _envs.put(AppEnvironment.APP_SPEC_FACTORY.toString(), className);

  }

  public void setMainClass(String serviceName, String serviceMainClass) {
    _envs.put(serviceName + "_mainClass", serviceMainClass);
  }
}
