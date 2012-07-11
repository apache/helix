package com.linkedin.helix.controller.restlet;

import java.io.StringReader;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;


public class ZNRecordUpdateResource  extends Resource
{
  public static final String UPDATEKEY = "ZNRecordUpdate";
  private static Logger LOG = Logger.getLogger(ZNRecordUpdateResource.class);
  @Override
  public boolean allowGet()
  {
    return false;
  }

  @Override
  public boolean allowPost()
  {
    return false;
  }

  @Override
  public boolean allowPut()
  {
    return true;
  }

  @Override
  public boolean allowDelete()
  {
    return false;
  }
  
  @Override
  public void storeRepresentation(Representation entity)
  {
    try
    {
      ZKPropertyTransferServer server = (ZKPropertyTransferServer) getRequest().getAttributes().get(ZKPropertyTransferServer.SERVER);
      
      Form form = new Form(entity);
      String jsonPayload = form.getFirstValue(UPDATEKEY, true);
      
      StringReader sr = new StringReader(jsonPayload);
      ObjectMapper mapper = new ObjectMapper();
      ZNRecordUpdate holder = mapper.readValue(sr, ZNRecordUpdate.class);      
      server.enqueueData(holder);
      LOG.trace("Received " + holder.getPath() + " from " + getRequest().getClientInfo().getAddress());
      
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    catch(Exception e)
    {
      LOG.error("", e);
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
    }
  }
}
