package com.linkedin.helix.controller.restlet;

import java.io.StringWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.Client;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;

public class ZkPropertyTransferClient
{
  private static Logger LOG = Logger.getLogger(ZkPropertyTransferClient.class);
  
  public static final int DEFAULT_MAX_CONCURRENTTASKS = 2;
  
  int _maxConcurrentTasks;
  ExecutorService _executorService;
  
  public ZkPropertyTransferClient(int maxConcurrentTasks)
  {
    _maxConcurrentTasks = maxConcurrentTasks;
    _executorService = Executors.newFixedThreadPool(_maxConcurrentTasks);
  }
  
  public void sendZNRecordUpdate(ZNRecordUpdate update, String webserviceUrl)
  {
    LOG.debug("Sending update to " + update.getPath() + " opcode: " + update.getOpcode());
    _executorService.submit(new SendZNRecordUpdateTask(update, webserviceUrl));
  }
  
  public void shutdown()
  {
    _executorService.shutdown();
  }
  
  class SendZNRecordUpdateTask implements Callable<Void>
  {
    ZNRecordUpdate _update;
    String _webServiceUrl;
    
    SendZNRecordUpdateTask(ZNRecordUpdate update, String webserviceUrl)
    {
      _update = update;
      _webServiceUrl = webserviceUrl;
    }
    
    @Override
    public Void call() throws Exception
    {
      LOG.debug("Actual sending update to " + _update.getPath() + " opcode: " + _update.getOpcode() +" to " + _webServiceUrl);
      Reference resourceRef = new Reference(_webServiceUrl);
      Request request = new Request(Method.PUT, resourceRef);
      
      ObjectMapper mapper = new ObjectMapper();
      StringWriter sw = new StringWriter();
      try
      {
        mapper.writeValue(sw, _update);
      }
      catch (Exception e)
      {
        LOG.error("",e);
      }

      request.setEntity(
          ZNRecordUpdateResource.UPDATEKEY + "=" + sw, MediaType.APPLICATION_ALL);
      Client client = new Client(Protocol.HTTP);
      // This is a sync call. See com.noelios.restlet.http.StreamClientCall.sendRequest()
      Response response = client.handle(request);
      
      if(response.getStatus() != Status.SUCCESS_OK)
      {
        LOG.error("Status : " + response.getStatus());
      }
      return null;
    }
  }
}
