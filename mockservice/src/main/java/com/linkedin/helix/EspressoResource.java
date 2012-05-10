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
package com.linkedin.helix;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.helix.tools.ClusterSetup;

public class EspressoResource extends Resource {
	
	private static final int POST_BODY_BUFFER_SIZE = 1024*10;
	
	private static final Logger logger = Logger
			.getLogger(EspressoResource.class);
	
	Context _context;
	
	 public EspressoResource(Context context,
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
		 System.out.println("PutResource.allowGet()");
	    return true;
	  }
	  
	  public boolean allowPost()
	  {
		  System.out.println("PutResource.allowPost()");
	    return true;
	  }
	  
	  public boolean allowPut()
	  {
		  System.out.println("PutResource.allowPut()");
	    return true;
	  }
	  
	  public boolean allowDelete()
	  {
	    return false;
	  }
	  
	  /*
	   * Handle get requests
	   * @see org.restlet.resource.Resource#represent(org.restlet.resource.Variant)
	   */
	  public Representation represent(Variant variant)
	  {
	    StringRepresentation presentation = null;
	    try
	    {
	    	String databaseId = (String)getRequest().getAttributes().get(MockEspressoService.DATABASENAME);
	    	String tableId = (String)getRequest().getAttributes().get(MockEspressoService.TABLENAME);
	    	String resourceId = (String)getRequest().getAttributes().get(MockEspressoService.RESOURCENAME);
	    	String subResourceId = (String)getRequest().getAttributes().get(MockEspressoService.SUBRESOURCENAME);
	    	logger.debug("Done getting request components");
	    	logger.debug("method: "+getRequest().getMethod());
	    	String composedKey = databaseId + tableId + resourceId; // + subResourceId;
	    	EspressoStorageMockNode mock = (EspressoStorageMockNode)_context.getAttributes().get(MockEspressoService.CONTEXT_MOCK_NODE_NAME);

	    	
	    	if (getRequest().getMethod() == Method.PUT) {
	    		logger.debug("processing PUT");
	    		Reader postBodyReader;
	    		//TODO: get to no fixed size on buffer
	    		char[] postBody = new char[POST_BODY_BUFFER_SIZE];	    		
	    		postBodyReader = getRequest().getEntity().getReader();
	    		postBodyReader.read(postBody);
	    		logger.debug("postBody: "+new String(postBody));
	    		mock.doPut(databaseId, composedKey, new String(postBody));
	    		presentation = new StringRepresentation("Put succeeded", MediaType.APPLICATION_JSON);
	    	}
	    	else if (getRequest().getMethod() == Method.GET) {
	    		logger.debug("processing GET");
	    		String result = mock.doGet(databaseId, composedKey);
			      logger.debug("result: "+result);
			      if (result == null) {
			    	  presentation = new StringRepresentation("Record not found", MediaType.APPLICATION_JSON);
			    	  getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,"Record not found");
			      }
			      else {
			    	  getResponse().setStatus(Status.SUCCESS_OK,"Success");
			    	  presentation = new StringRepresentation(result, MediaType.APPLICATION_JSON);
			      }
	    	}
	    }

	    catch (IOException e) {
	    	presentation = new StringRepresentation(e.getMessage(), MediaType.APPLICATION_JSON);
	    	e.printStackTrace();
	    }

	    catch(Exception e)
	    {
	    	String error = "Error with op"; //ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
	    	presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);	      
	    	e.printStackTrace();
	    }  
	    return presentation;
	  }
	  
	  /*
	   * Handle put requests (non-Javadoc)
	   * @see org.restlet.resource.Resource#storeRepresentation(org.restlet.resource.Representation)
	   */
	 public void storeRepresentation(Representation entity) throws ResourceException {
		 logger.debug("in storeRepresentation");
		 StringRepresentation presentation = null;
		// try {
		 Form requestHeaders = (Form) getRequest().getAttributes().get("org.restlet.http.headers");
		 Map<String, String> headerMap = requestHeaders.getValuesMap();
		 logger.debug("HEADERS MAP");
		 for (String key : headerMap.keySet()) {
			 logger.debug(key+" : "+headerMap.get(key));
		 }
	//	} catch (IOException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		//}   
		 try
		    {
		    	logger.debug("in PutResource handle");
		    	String databaseId = (String)getRequest().getAttributes().get(MockEspressoService.DATABASENAME);
		    	String tableId = (String)getRequest().getAttributes().get(MockEspressoService.TABLENAME);
		    	String resourceId = (String)getRequest().getAttributes().get(MockEspressoService.RESOURCENAME);
		    	String subResourceId = (String)getRequest().getAttributes().get(MockEspressoService.SUBRESOURCENAME);
		    	logger.debug("Done getting request components");
		    	logger.debug("method: "+getRequest().getMethod());
		    	String composedKey = databaseId + tableId + resourceId; // + subResourceId;
		    	EspressoStorageMockNode mock = (EspressoStorageMockNode)_context.getAttributes().get(MockEspressoService.CONTEXT_MOCK_NODE_NAME);

		    	if (getRequest().getMethod() == Method.PUT) {
		    		logger.debug("processing PUT");
		    		Reader postBodyReader;
		    		//TODO: get to no fixed size on buffer
		    		char[] postBody = new char[POST_BODY_BUFFER_SIZE];
		    		postBodyReader = getRequest().getEntity().getReader();
		    		postBodyReader.read(postBody);
		    		logger.debug("postBody: "+new String(postBody));
		    		mock.doPut(databaseId, composedKey, new String(postBody));
		    		presentation = new StringRepresentation("Put succeeded", MediaType.APPLICATION_JSON);
		    	}
		    	else if (getRequest().getMethod() == Method.GET) {
		    		logger.debug("Processing GET");
		    		String result = mock.doGet(databaseId, composedKey);
				      logger.debug("result: "+result);
				      if (result == null) {
				    	  presentation = new StringRepresentation("Record not found", MediaType.APPLICATION_JSON);
				      }
				      else {
				    	  presentation = new StringRepresentation(result, MediaType.APPLICATION_JSON);
				      }
		    	}
		    }

		    catch (IOException e) {
		    	presentation = new StringRepresentation(e.getMessage(), MediaType.APPLICATION_JSON);
		    	e.printStackTrace();
		    }

		    catch(Exception e)
		    {
		    	String error = "Error with op";
		    	presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);	      
		    	e.printStackTrace();
		    }  
		    finally {
		    	entity.release();
		    }
	 }
}
