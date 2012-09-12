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
import java.util.ArrayList;
import java.util.Arrays;
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
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class InstanceResource extends Resource
{
  private final static Logger LOG = Logger.getLogger(InstanceResource.class);
  public static final String _partition = "partition";
  public static final String _resource = "resource";

  public InstanceResource(Context context, Request request, Response response)
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
      presentation = getInstanceRepresentation();
    }
    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("", e);
    }
    return presentation;
  }

  StringRepresentation getInstanceRepresentation() throws JsonGenerationException,
      JsonMappingException,
      IOException
  {
    String clusterName = (String) getRequest().getAttributes().get("clusterName");
    String instanceName = (String) getRequest().getAttributes().get("instanceName");
    Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);;
    
    String message =
        ClusterRepresentationUtil.getClusterPropertyAsString(zkClient,
                                                             clusterName,
                                                             MediaType.APPLICATION_JSON,
                                                             keyBuilder.instanceConfig(instanceName));

    StringRepresentation representation =
        new StringRepresentation(message, MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public void acceptRepresentation(Representation entity)
  {
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String instanceName = (String) getRequest().getAttributes().get("instanceName");

      Form form = new Form(entity);
      Map<String, String> paraMap = ClusterRepresentationUtil.getFormJsonParameters(form);
      String command = paraMap.get(ClusterRepresentationUtil._managementCommand);
      if(command.equalsIgnoreCase(ClusterSetup.enableInstance))
      {
        paraMap =
            ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form,
                                                                               ClusterSetup.enableInstance);
  
        boolean enabled =
            Boolean.parseBoolean(paraMap.get(ClusterRepresentationUtil._enabled));
  
        ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
        ClusterSetup setupTool = new ClusterSetup(zkClient);
        setupTool.getClusterManagementTool().enableInstance(clusterName,
                                                            instanceName,
                                                            enabled);
      }
      else if(command.equalsIgnoreCase(ClusterSetup.enablePartition))
      {
        paraMap =
            ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form,
                                                                               ClusterSetup.enablePartition);
        if(! paraMap.containsKey(ClusterRepresentationUtil._enabled))
        {
          throw new HelixException("Json parameters does not contain '"+ ClusterRepresentationUtil._enabled + "'");
        }
        if(! paraMap.containsKey(_partition))
        {
          throw new HelixException("Json parameters does not contain '"+ _partition + "'");
        }
        if(! paraMap.containsKey(_resource))
        {
          throw new HelixException("Json parameters does not contain '"+ _resource + "'");
        }
        boolean enabled =
            Boolean.parseBoolean(paraMap.get(ClusterRepresentationUtil._enabled));
        String[] partitions = paraMap.get(_partition).split(";");
        String resource = paraMap.get(_resource);
  
        ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
        ClusterSetup setupTool = new ClusterSetup(zkClient);
        setupTool.getClusterManagementTool().enablePartition(enabled, clusterName, instanceName, resource, Arrays.asList(partitions));
      }
      else if(command.equalsIgnoreCase(ClusterSetup.resetPartition))
      {
        paraMap =
            ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form,
                                                                               ClusterSetup.resetPartition);
        if(! paraMap.containsKey(_partition))
        {
          throw new HelixException("Json parameters does not contain '"+ _partition + "'");
        }
        if(! paraMap.containsKey(_resource))
        {
          throw new HelixException("Json parameters does not contain '"+ _resource + "'");
        }
        String partition = paraMap.get(_partition);
        String resource = paraMap.get(_resource);
  
        ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
        ClusterSetup setupTool = new ClusterSetup(zkClient);
        List<String> resetPartitionNames = new ArrayList<String>();
        resetPartitionNames.add(partition);
        setupTool.getClusterManagementTool().resetPartition(clusterName, instanceName, resource, resetPartitionNames);
      }
      getResponse().setEntity(getInstanceRepresentation());
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("", e);
    }
  }

  @Override
  public void removeRepresentations()
  {
    try
    {
      String clusterName = (String) getRequest().getAttributes().get("clusterName");
      String instanceName = (String) getRequest().getAttributes().get("instanceName");
      ZkClient zkClient = (ZkClient)getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);;
      ClusterSetup setupTool = new ClusterSetup(zkClient);
      setupTool.dropInstanceFromCluster(clusterName, instanceName);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("", e);
    }
  }
}
