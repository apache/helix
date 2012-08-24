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

import org.apache.log4j.Logger;
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
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;


public class ClustersResource extends Resource
{
  private final static Logger LOG = Logger.getLogger(ClustersResource.class);
  public static final String _clusterName = "clusterName";
  public static final String _grandCluster = "grandCluster";
  public ClustersResource(Context context,
            Request request,
            Response response)
  {
    super(context, request, response);
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    // handle(request,response);
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
      presentation = getClustersRepresentation();
    }
    catch(Exception e)
    {
      LOG.error("", e);
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      e.printStackTrace();
    }
    return presentation;
  }

  StringRepresentation getClustersRepresentation() throws JsonGenerationException, JsonMappingException, IOException
  {
    ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
    ClusterSetup setupTool = new ClusterSetup(zkClient);
    List<String> clusters = setupTool.getClusterManagementTool().getClusters();

    ZNRecord clustersRecord = new ZNRecord("Clusters Summary");
    clustersRecord.setListField("clusters", clusters);
    StringRepresentation representation = new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(clustersRecord), MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      Form form = new Form(entity);
      Map<String, String> jsonParameters
        = ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form, ClusterSetup.addCluster);

      if(! jsonParameters.containsKey(_clusterName))
      {
        throw new HelixException("Json parameters does not contain '"+ _clusterName + "'");
      }
      ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkClient);
      setupTool.addCluster(jsonParameters.get(_clusterName), false);
      
      getResponse().setEntity(getClustersRepresentation());
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    catch(Exception e)
    {
      LOG.error("", e);
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
  }
  
  @Override
  public void removeRepresentations()
  {
    
  }
}
