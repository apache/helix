/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.webapp.resources;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.util.ZKClientPool;
import com.linkedin.helix.webapp.RestAdminApplication;

public class ClusterResource extends Resource
{
  public static final String _clusterName  = "clusterName";
  public static final String _grandCluster = "grandCluster";
  public static final String _enabled = "enabled";

  public ClusterResource(Context context, Request request, Response response)
  {
    super(context, request, response);
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
  }

  @Override
  public boolean allowGet()
  {
    return true;
  }

  @Override
  public boolean allowPost()
  {
    return true;
  }

  @Override
  public boolean allowPut()
  {
    return false;
  }

  @Override
  public boolean allowDelete()
  {
    return true;
  }

  @Override
  public Representation represent(Variant variant)
  {
    StringRepresentation presentation = null;
    try
    {
      String zkServer =
          (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      presentation = getClusterRepresentation(zkServer, clusterName);
    }

    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      e.printStackTrace();
    }
    return presentation;
  }

  StringRepresentation getClusterRepresentation(String zkServerAddress, String clusterName) throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
    List<String> instances =
        setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);

    ZNRecord clusterSummayRecord = new ZNRecord("Cluster Summary");
    clusterSummayRecord.setListField("participants", instances);

    List<String> resources =
        setupTool.getClusterManagementTool().getResourcesInCluster(clusterName);
    clusterSummayRecord.setListField("resources", resources);

    List<String> models =
        setupTool.getClusterManagementTool().getStateModelDefs(clusterName);
    clusterSummayRecord.setListField("stateModelDefs", models);

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName,
                                new ZkBaseDataAccessor(ZKClientPool.getZkClient(zkServerAddress)));
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader != null)
    {
      clusterSummayRecord.setSimpleField("LEADER", leader.getInstanceName());
    }
    else
    {
      clusterSummayRecord.setSimpleField("LEADER", "");
    }
    StringRepresentation representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(clusterSummayRecord),
                                 MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String zkServer =
          (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      Form form = new Form(entity);
      Map<String, String> jsonParameters =
          ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(
            form, ClusterSetup.activateCluster);

      if (!jsonParameters.containsKey(_grandCluster))
      {
        throw new HelixException("Json parameters does not contain '" + _grandCluster
            + "'");
      }
      
      if (!jsonParameters.containsKey(_enabled))
      {
        throw new HelixException("Json parameters does not contain '" + _enabled
            + "'");
      }

      String grandCluster = jsonParameters.get(_grandCluster);
      boolean enabled = Boolean.parseBoolean(jsonParameters.get(_enabled));
      ClusterSetup setupTool = new ClusterSetup(zkServer);
      List<String> grandClusterResourceGroups =
          setupTool.getClusterManagementTool().getResourcesInCluster(grandCluster);
      if (grandClusterResourceGroups.contains(clusterName))
      {
        throw new HelixException("Grand cluster " + grandCluster
            + " already have a resourceGroup for " + clusterName);
      }
      setupTool.activateCluster(clusterName, grandCluster, enabled);
      // add cluster
      getResponse().setEntity(getClusterRepresentation(zkServer, clusterName));
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
  }

  @Override
  public void removeRepresentations()
  {
    try
    {
      String zkServer =
          (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      ClusterSetup setupTool = new ClusterSetup(zkServer);
      setupTool.deleteCluster(clusterName);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
  }
}
