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
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;


public class ClustersResource extends Resource
{
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
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      e.printStackTrace();
    }
    return presentation;
  }

  StringRepresentation getClustersRepresentation() throws JsonGenerationException, JsonMappingException, IOException
  {
    String zkServer = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
    ClusterSetup setupTool = new ClusterSetup(zkServer);
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
      String zkServer = (String)getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
      Form form = new Form(entity);
      Map<String, String> jsonParameters
        = ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form, ClusterRepresentationUtil._addClusterCommand);

      if(! jsonParameters.containsKey(_clusterName))
      {
        throw new HelixException("Json parameters does not contain '"+ _clusterName + "'");
      }
      String grandCluster = null;
      if(jsonParameters.containsKey(_grandCluster))
      {
        grandCluster = jsonParameters.get(_grandCluster);
      }
      ClusterSetup setupTool = new ClusterSetup(zkServer);
      if(grandCluster == null)
      {
        setupTool.addCluster(jsonParameters.get(_clusterName), false);
      }
      else
      {
        setupTool.addCluster(jsonParameters.get(_clusterName), false, grandCluster);
      }
      // add cluster
      getResponse().setEntity(getClustersRepresentation());
      getResponse().setStatus(Status.SUCCESS_OK);
    }

    catch(Exception e)
    {
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
