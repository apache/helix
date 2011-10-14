package com.linkedin.clustermanager;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.clustermanager.tools.ClusterSetup;

public class PostResource extends Resource {
	
	private static final int POST_BODY_BUFFER_SIZE = 1024*1024;
	
	private static final Logger logger = Logger
			.getLogger(PostResource.class);
	
	 public PostResource(Context context,
	            Request request,
	            Response response) 
	  {
	    super(context, request, response);
	    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
	    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
	  }
	 public boolean allowGet()
	  {
	    return false;
	  }
	  
	  public boolean allowPost()
	  {
	    return false;
	  }
	  
	  public boolean allowPut()
	  {
	    return true;
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
	      String databaseId = (String)getRequest().getAttributes().get(MockEspressoService.DATABASENAME);
	      String tableId = (String)getRequest().getAttributes().get(MockEspressoService.TABLENAME);
	      String resourceId = (String)getRequest().getAttributes().get(MockEspressoService.RESOURCENAME);
	      //String subResourceId = (String)getRequest().getAttributes().get(MockEspressoService.SUBRESOURCENAME);
	      String postBody = "xxx";
	      //TODO: figure out how to actually get post body
	      logger.debug("Done getting request components");
	      String composedKey = databaseId + tableId + resourceId; // + subResourceId;
	      //String response = MockEspressoService.doGet(composedKey);
	      //logger.debug("response: "+response);
	      //presentation = new StringRepresentation(response, MediaType.APPLICATION_JSON);
	    }
	    
	    catch(Exception e)
	    {
	      String error = "Error with get"; //ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
	      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
	      
	      e.printStackTrace();
	    }  
	    return presentation;
	  }
	  
	 public void storeRepresentation(Representation entity) throws ResourceException {
		 logger.debug("in storeRepresentation");
		 StringRepresentation presentation = null;
		 try
		 {
			 Request xxx = getRequest();
			 logger.debug("got message");
			 Reader postBodyReader = entity.getReader();
			 //char[] zzz = new char[100]; 
			 //yyy.read(zzz);
			 //TODO: accomodate post bodies that are arbitrarily long
			 
			 /*
			 char[] postBody = new char[POST_BODY_BUFFER_SIZE];
			 postBodyReader.read(postBody);
			 logger.debug("postBodyBuffer: "+new String(postBody));
			 */
			 String postBody = "abc";
			 
			 String databaseId = (String)getRequest().getAttributes().get(MockEspressoService.DATABASENAME);
			 String tableId = (String)getRequest().getAttributes().get(MockEspressoService.TABLENAME);
			 String resourceId = (String)getRequest().getAttributes().get(MockEspressoService.RESOURCENAME);
			 String composedKey = databaseId + tableId + resourceId; // + subResourceId;
			 //!!!MockEspressoService.doPut(composedKey, postBody);
			 /*
			 if (MockEspressoService._mockNode == null) {
				 logger.debug("_mockNode is null");
			 }
			 */
			 //logger.debug("response: "+response);
			 String response = "Did put";
			 presentation = new StringRepresentation(response, MediaType.APPLICATION_JSON);

			 getResponse().setStatus(Status.SUCCESS_OK);
		 }
		 catch(Exception e) 
		 {
			 //String error = "Error with put";
			 getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			 //presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
		      e.printStackTrace();
		      
			 
		 }
		 //return presentation;
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
