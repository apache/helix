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
package org.apache.helix;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.apache.helix.tools.ClusterSetup;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.restlet.Component;
import org.restlet.Context;
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


public class StopServiceResource extends Resource {


	private static final Logger logger = Logger
			.getLogger(StopServiceResource.class);

	Context _context;

	public StopServiceResource(Context context,
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
		return false;
	}

	public boolean allowPut()
	{
		System.out.println("PutResource.allowPut()");
		return false;
	}

	public boolean allowDelete()
	{
		return false;
	}


	class StopThread implements Runnable {

		Component _component;
		MockNode _mockNode;
		
		StopThread(Component c, MockNode m) {
			_component = c;
			_mockNode = m;
		}
		
		@Override
		public void run() {
			try {
				//sleep for 1 second, then end service
				Thread.sleep(1000);
				_component.stop();
				_mockNode.disconnect();
				System.exit(0);
			} catch (Exception e) {
				logger.error("Unable to stop service: "+e);
				e.printStackTrace();
			}
		}
		
	}
	
	//XXX: handling both gets and puts here for now
	public Representation represent(Variant variant)
	{
		System.out.println("StopServiceResource.represent()");
		StringRepresentation presentation = null;
		try
		{
			logger.debug("in represent, stopping service");
			Component component = (Component)_context.getAttributes().get(MockEspressoService.COMPONENT_NAME);
			EspressoStorageMockNode mock = (EspressoStorageMockNode)_context.getAttributes().get(MockEspressoService.CONTEXT_MOCK_NODE_NAME);
			presentation = new StringRepresentation("Stopping in 1 second", MediaType.APPLICATION_JSON);
			Thread stopper = new Thread(new StopThread(component, mock));
			stopper.start();
		}

		catch(Exception e)
		{
			String error = "Error shutting down";
			presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);	      
			e.printStackTrace();
		}  
		return presentation;
	}

	public void storeRepresentation(Representation entity) throws ResourceException {

	}



}
