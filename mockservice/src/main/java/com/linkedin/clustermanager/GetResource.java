package com.linkedin.clustermanager;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.clustermanager.tools.ClusterSetup;

public class GetResource extends Resource {
	
	private static final Logger logger = Logger
			.getLogger(GetResource.class);
	
	 public GetResource(Context context,
	            Request request,
	            Response response) 
	  {
	    super(context, request, response);
	    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
	    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	  }
	 public boolean allowGet()
	  {
	    return true;
	  }
	  
	  public boolean allowPost()
	  {
	    return false;
	  }
	  
	  public boolean allowPut()
	  {
	    return false;
	  }
	  
	  public boolean allowDelete()
	  {
	    return false;
	  }
	  
	
	 public Representation represent(Variant variant)
	  {
	    StringRepresentation presentation = null;
	    try
	    {
	      String zkServer = (String)getContext().getAttributes().get(MockEspressoService.ZKSERVERADDRESS);
	      logger.debug("zkServer: "+zkServer);
	      //Context currContext = getContext();
	      //Request currReq = getRequest();
	      //logger.debug("xxx");
	      String clusterName = (String)getRequest().getAttributes().get(MockEspressoService.CLUSTER_NAME);
	      logger.debug("clusterName: "+clusterName);
	      presentation = getClusterRepresentation(zkServer, clusterName);
	    }
	    
	    catch(Exception e)
	    {
	      String error = "error"; //ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
	      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
	      
	      e.printStackTrace();
	    }  
	    return presentation;
	  }
	  
	  StringRepresentation getClusterRepresentation(String zkServerAddress, String clusterName) throws JsonGenerationException, JsonMappingException, IOException
	  {
	    ClusterSetup setupTool = new ClusterSetup(zkServerAddress);
	    List<String> instances = setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);
	    
	    ZNRecord clusterSummayRecord = new ZNRecord("cluster summary");
	    //clusterSummayRecord.setId("cluster summary");
	    clusterSummayRecord.setListField("instances", instances);
	    
	    List<String> hostedEntities = setupTool.getClusterManagementTool().getResourceGroupsInCluster(clusterName);
	    clusterSummayRecord.setListField("resourceGroups", hostedEntities);
	    
	    List<String> models = setupTool.getClusterManagementTool().getStateModelDefs(clusterName);
	    clusterSummayRecord.setListField("stateModels", models);
	    
	    StringRepresentation representation = new StringRepresentation("{xxx}"); //ClusterRepresentationUtil.ZNRecordToJson(clusterSummayRecord), MediaType.APPLICATION_JSON);
	    
	    return representation;
	  }
}
