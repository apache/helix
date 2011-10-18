package com.linkedin.clustermanager;

import org.apache.log4j.Logger;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.StringRepresentation;

public class GetRestlet extends Restlet {
	
	private static final Logger logger = Logger
			.getLogger(GetRestlet.class);
	
	EspressoStorageMockNode _mockNode;
	
	public GetRestlet(Context context, EspressoStorageMockNode mockNode) {
		super(context);
		_mockNode = mockNode;
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
	
	@Override
    public void handle(Request request, Response response)
    {
		logger.debug("in GetRestlet handle");
		String databaseId = (String)request.getAttributes().get(MockEspressoService.DATABASENAME);
	      String tableId = (String)request.getAttributes().get(MockEspressoService.TABLENAME);
	      String resourceId = (String)request.getAttributes().get(MockEspressoService.RESOURCENAME);
	      String subResourceId = (String)request.getAttributes().get(MockEspressoService.SUBRESOURCENAME);
	      logger.debug("Done getting request components");
	      String composedKey = databaseId + tableId + resourceId + subResourceId;
	      String result = _mockNode.doGet(composedKey);
	      logger.debug("result: "+result);
	      response.setEntity(result, MediaType.APPLICATION_JSON);
    }
}
