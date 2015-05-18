package org.apache.helix.webapp.resources;

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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.webapp.RestAdminApplication;
import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

public class ConfigResource extends ServerResource {
  private final static Logger LOG = Logger.getLogger(ConfigResource.class);

  public ConfigResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  String getValue(String key) {
    return (String) getRequest().getAttributes().get(key);
  }

  static StringRepresentation getConfigScopes() throws Exception {
    StringRepresentation representation = null;
    ZNRecord record = new ZNRecord("Config");

    List<String> scopeList =
        Arrays.asList(ConfigScopeProperty.CLUSTER.toString(),
            ConfigScopeProperty.RESOURCE.toString(), ConfigScopeProperty.PARTICIPANT.toString(),
            ConfigScopeProperty.PARTITION.toString());
    record.setListField("scopes", scopeList);

    representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(record),
            MediaType.APPLICATION_JSON);

    return representation;
  }

  StringRepresentation getConfigKeys(ConfigScopeProperty scopeProperty, String... keys)
      throws Exception {
    StringRepresentation representation = null;
    // String clusterName = getValue("clusterName");

    ZkClient zkClient = (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
    ClusterSetup setupTool = new ClusterSetup(zkClient);
    HelixAdmin admin = setupTool.getClusterManagementTool();
    ZNRecord record = new ZNRecord(scopeProperty + " Config");

    HelixConfigScope scope = new HelixConfigScopeBuilder(scopeProperty, keys).build();
    // List<String> configKeys = admin.getConfigKeys(scopeProperty, clusterName, keys);
    List<String> configKeys = admin.getConfigKeys(scope);
    record.setListField(scopeProperty.toString(), configKeys);

    representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(record),
            MediaType.APPLICATION_JSON);

    return representation;
  }

  StringRepresentation getConfigs(// ConfigScope scope,
      ConfigScopeProperty scopeProperty, String... keys) throws Exception {
    StringRepresentation representation = null;
    // String clusterName = getValue("clusterName");

    ZkClient zkClient = (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
    ClusterSetup setupTool = new ClusterSetup(zkClient);
    HelixAdmin admin = setupTool.getClusterManagementTool();
    ZNRecord record = new ZNRecord(scopeProperty + " Config");

    HelixConfigScope scope = new HelixConfigScopeBuilder(scopeProperty, keys).build();
    List<String> configKeys = admin.getConfigKeys(scope);
    Map<String, String> configs = admin.getConfig(scope, configKeys);
    record.setSimpleFields(configs);

    representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(record),
            MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation get() {
    StringRepresentation representation = null;

    String clusterName = getValue("clusterName");
    String scopeStr = getValue("scope");
    try {
      if (scopeStr == null) {
        // path is "/clusters/{clusterName}/configs"
        return getConfigScopes();
      }

      scopeStr = scopeStr.toUpperCase();

      ConfigScopeProperty scopeProperty = ConfigScopeProperty.valueOf(scopeStr);
      switch (scopeProperty) {
      case CLUSTER:
      case PARTICIPANT:
      case RESOURCE:
        String scopeKey1 = getValue("scopeKey1");
        if (scopeKey1 == null) {
          // path is "/clusters/{clusterName}/configs/cluster|participant|resource"
          representation = getConfigKeys(scopeProperty, clusterName);
        } else {
          // path is "/clusters/{clusterName}/configs/cluster|participant|resource/
          // {clusterName}|{participantName}|{resourceName}"
          // ConfigScope scope;
          // if (scopeProperty == ConfigScopeProperty.CLUSTER)
          // {
          // scope = new ConfigScopeBuilder().build(scopeProperty, clusterName);
          // }
          // else
          // {
          // scope = new ConfigScopeBuilder().build(scopeProperty, clusterName, scopeKey1);
          // }
          representation = getConfigs(scopeProperty, clusterName, scopeKey1);
        }
        break;
      case PARTITION:
        scopeKey1 = getValue("scopeKey1");
        String scopeKey2 = getValue("scopeKey2");
        if (scopeKey1 == null) {
          // path is "/clusters/{clusterName}/configs/partition"
          throw new HelixException("Missing resourceName");
        } else if (scopeKey2 == null) {
          // path is "/clusters/{clusterName}/configs/partition/resourceName"
          representation = getConfigKeys(scopeProperty, clusterName, scopeKey1);
        } else {
          // path is
          // "/clusters/{clusterName}/configs/partition/resourceName/partitionName"
          // ConfigScope scope =
          // new ConfigScopeBuilder().build(scopeProperty,
          // clusterName,
          // scopeKey1,
          // scopeKey2);
          representation = getConfigs(scopeProperty, clusterName, scopeKey1, scopeKey2);
        }
        break;
      default:
        break;
      }
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      representation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      LOG.error("", e);
    }

    return representation;
  }

  /**
   * set or remove configs depends on "command" field of jsonParameters in POST body
   * @param entity
   * @param scopeStr
   * @throws Exception
   */
  void setConfigs(Representation entity, ConfigScopeProperty type, String scopeArgs)
      throws Exception {
    JsonParameters jsonParameters = new JsonParameters(entity);
    String command = jsonParameters.getCommand();

    ZkClient zkClient = (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
    ClusterSetup setupTool = new ClusterSetup(zkClient);
    if (command.equalsIgnoreCase(ClusterSetup.setConfig)) {
      jsonParameters.verifyCommand(ClusterSetup.setConfig);
      String propertiesStr = jsonParameters.getParameter(JsonParameters.CONFIGS);

      // setupTool.setConfig(scopeStr, propertiesStr);
      setupTool.setConfig(type, scopeArgs, propertiesStr);
    } else if (command.equalsIgnoreCase(ClusterSetup.removeConfig)) {
      jsonParameters.verifyCommand(ClusterSetup.removeConfig);
      String propertiesStr = jsonParameters.getParameter(JsonParameters.CONFIGS);

      setupTool.removeConfig(type, scopeArgs, propertiesStr);
    } else {
      throw new HelixException("Unsupported command: " + command + ". Should be one of ["
          + ClusterSetup.setConfig + ", " + ClusterSetup.removeConfig + "]");

    }

    getResponse().setEntity(get());
    getResponse().setStatus(Status.SUCCESS_OK);
  }


@Override
  public Representation post(Representation entity) {
    String clusterName = getValue("clusterName");

    String scopeStr = getValue("scope").toUpperCase();
    try {
      ConfigScopeProperty scopeProperty = ConfigScopeProperty.valueOf(scopeStr);

      switch (scopeProperty) {
      case CLUSTER:
        String scopeArgs = clusterName;
        setConfigs(entity, scopeProperty, scopeArgs);
        break;
      case PARTICIPANT:
      case RESOURCE:
        String scopeKey1 = getValue("scopeKey1");

        if (scopeKey1 == null) {
          throw new HelixException("Missing resourceName|participantName");
        } else {
          scopeArgs = clusterName + "," + scopeKey1;
          setConfigs(entity, scopeProperty, scopeArgs);
        }
        break;
      case PARTITION:
        scopeKey1 = getValue("scopeKey1");
        String scopeKey2 = getValue("scopeKey2");
        if (scopeKey1 == null || scopeKey2 == null) {
          throw new HelixException("Missing resourceName|partitionName");
        } else {
          scopeArgs = clusterName + "," + scopeKey1 + "," + scopeKey2;
          setConfigs(entity, scopeProperty, scopeArgs);
        }
        break;
      default:
        break;
      }
    } catch (Exception e) {
      LOG.error("Error in posting " + entity, e);
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    return null;
  }
}
