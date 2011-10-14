package com.linkedin.clustermanager;

import java.io.IOException;
import java.io.Reader;

import org.apache.log4j.Logger;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.StringRepresentation;

public class PutRestlet extends Restlet {
	
	private static final Logger logger = Logger
			.getLogger(PutRestlet.class);
	
	public static final int POST_BODY_BUFFER_SIZE = 1024;
	
	EspressoStorageMockNode _mockNode;
	
	public PutRestlet(Context context, EspressoStorageMockNode mockNode) {
		super(context);
		_mockNode = mockNode;
	}
	
	@Override
	public void handle(Request request, Response response)
	{
		logger.debug("in PutRestlet handle");
		String databaseId = (String)request.getAttributes().get(MockEspressoService.DATABASENAME);
		String tableId = (String)request.getAttributes().get(MockEspressoService.TABLENAME);
		String resourceId = (String)request.getAttributes().get(MockEspressoService.RESOURCENAME);
		String subResourceId = (String)request.getAttributes().get(MockEspressoService.SUBRESOURCENAME);
		logger.debug("Done getting request components");
		String composedKey = databaseId + tableId + resourceId + subResourceId;
		Reader postBodyReader;
		//TODO: get to no fixed size on buffer
		char[] postBody = new char[POST_BODY_BUFFER_SIZE];
		try {
			postBodyReader = request.getEntity().getReader();
			 postBodyReader.read(postBody);
			 logger.debug("postBodyBuffer: "+new String(postBody));
		} catch (IOException e) {
			response.setStatus(Status.SERVER_ERROR_INTERNAL, "Could not read request body");
			e.printStackTrace();
			return;
		}
		_mockNode.doPut(composedKey, new String(postBody));
		response.setStatus(Status.SUCCESS_OK, "Put succeeded");
	}
}
