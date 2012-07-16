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
  
  public static final String USE_PROPERTYTRANSFER = "UsePropertyTransfer";
  
  int _maxConcurrentTasks;
  ExecutorService _executorService;
  Client[] _clients;
  int _requestCount = 0;
  
  public ZkPropertyTransferClient(int maxConcurrentTasks)
  {
    _maxConcurrentTasks = maxConcurrentTasks;
    _executorService = Executors.newFixedThreadPool(_maxConcurrentTasks);
    _clients = new Client[_maxConcurrentTasks];
    for(int i = 0; i< _clients.length; i++)
    {
      _clients[i] = new Client(Protocol.HTTP);
    }
  }
  
  public void sendZNRecordUpdate(ZNRecordUpdate update, String webserviceUrl)
  {
    LOG.debug("Sending update to " + update.getPath() + " opcode: " + update.getOpcode());
    update.getRecord().setSimpleField(USE_PROPERTYTRANSFER, "true");
    _executorService.submit(new SendZNRecordUpdateTask(update, webserviceUrl, _clients[_requestCount % _maxConcurrentTasks]));
    _requestCount ++;
  }
  
  public void shutdown()
  {
    _executorService.shutdown();
    for(Client client: _clients)
    {
      try
      {
        client.stop();
      }
      catch (Exception e)
      {
        LOG.error("", e);
      }
    }
  }
  
  class SendZNRecordUpdateTask implements Callable<Void>
  {
    ZNRecordUpdate _update;
    String _webServiceUrl;
    Client _client;
    
    SendZNRecordUpdateTask(ZNRecordUpdate update, String webserviceUrl, Client client)
    {
      _update = update;
      _webServiceUrl = webserviceUrl;
      _client = client;
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
      // This is a sync call. See com.noelios.restlet.http.StreamClientCall.sendRequest()
      Response response = _client.handle(request);
      
      if(response.getStatus().getCode() != Status.SUCCESS_OK.getCode())
      {
        LOG.error("Status : " + response.getStatus());
      }
      return null;
    }
  }
}
