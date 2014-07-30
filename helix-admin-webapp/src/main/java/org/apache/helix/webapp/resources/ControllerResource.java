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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.StatusUpdateUtil.Level;
import org.apache.helix.webapp.RestAdminApplication;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

public class ControllerResource extends ServerResource {

  public ControllerResource()
  {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  StringRepresentation getControllerRepresentation(String clusterName)
      throws JsonGenerationException, JsonMappingException, IOException {
    Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkClient zkClient = (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

    ZNRecord record = null;
    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader != null) {
      record = leader.getRecord();
    } else {
      record = new ZNRecord("");
      DateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss.SSSSSS");
      String time = formatter.format(new Date());
      Map<String, String> contentMap = new TreeMap<String, String>();
      contentMap.put("AdditionalInfo", "No leader exists");
      record.setMapField(Level.HELIX_INFO + "-" + time, contentMap);
    }

    boolean paused = (accessor.getProperty(keyBuilder.pause()) == null ? false : true);
    record.setSimpleField(PropertyType.PAUSE.toString(), "" + paused);

    String retVal = ClusterRepresentationUtil.ZNRecordToJson(record);
    StringRepresentation representation =
        new StringRepresentation(retVal, MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      presentation = getControllerRepresentation(clusterName);
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      e.printStackTrace();
    }
    return presentation;
  }

  @Override
  public Representation post(Representation entity) {
    try {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkClient);

      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();

      if (command == null) {
        throw new HelixException("Could NOT find 'command' in parameterMap: "
            + jsonParameters._parameterMap);
      } else if (command.equalsIgnoreCase(ClusterSetup.enableCluster)) {
        boolean enabled = Boolean.parseBoolean(jsonParameters.getParameter(JsonParameters.ENABLED));

        setupTool.getClusterManagementTool().enableCluster(clusterName, enabled);
      } else {
        throw new HelixException("Unsupported command: " + command + ". Should be one of ["
            + ClusterSetup.enableCluster + "]");
      }

      getResponse().setEntity(getControllerRepresentation(clusterName));
      getResponse().setStatus(Status.SUCCESS_OK);

    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    return null;
  }
}
