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
	
	Context _context;
	
	 public GetResource(Context context,
	            Request request,
	            Response response) 
	  {
	    super(context, request, response);
	    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
	    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	    _context = context;
	  }
	 public boolean allowGet()
	  {
		System.out.println("GetResource.allowGet()");
	    return true;
	  }
	  
	  public boolean allowPost()
	  {
		  System.out.println("GetResource.allowPost()");
	    return false;
	  }
	  
	  public boolean allowPut()
	  {
		  System.out.println("GetResource.allowPut()");
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
	    	
	    	logger.debug("in GetResource handle");
			String databaseId = (String)getRequest().getAttributes().get(MockEspressoService.DATABASENAME);
		      String tableId = (String)getRequest().getAttributes().get(MockEspressoService.TABLENAME);
		      String resourceId = (String)getRequest().getAttributes().get(MockEspressoService.RESOURCENAME);
		      String subResourceId = (String)getRequest().getAttributes().get(MockEspressoService.SUBRESOURCENAME);
		      logger.debug("Done getting request components");
		      String composedKey = databaseId + tableId + resourceId + subResourceId;
		      EspressoStorageMockNode mock = (EspressoStorageMockNode)_context.getAttributes().get(MockEspressoService.CONTEXT_MOCK_NODE_NAME);
		      String result = mock.doGet(composedKey);
		      logger.debug("result: "+result);
		      //response.setEntity(result, MediaType.APPLICATION_JSON);
		      if (result == null) {
		    	  presentation = new StringRepresentation("Record not found", MediaType.APPLICATION_JSON);
		      }
		      else {
		    	  presentation = new StringRepresentation(result, MediaType.APPLICATION_JSON);
		      }
	    }
	    
	    catch(Exception e)
	    {
	      String error = "Error with get"; //ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
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
