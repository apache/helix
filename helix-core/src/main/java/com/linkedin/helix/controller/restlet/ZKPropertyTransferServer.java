package com.linkedin.helix.controller.restlet;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.Client;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;

public class ZKPropertyTransferServer extends Thread
{
  public static final String PORT = "port";
  public static final String SERVER = "ZKPropertyTransferServer";
  public static final int PERIOD = 30 * 1000;
  private static Logger LOG = Logger.getLogger(ZKPropertyTransferServer.class);
  
  final int _localWebservicePort;
  final HelixManager _manager;
  Timer _timer;
  AtomicReference<ConcurrentLinkedQueue<ZNRecordUpdate>> _dataQueueRef
    = new AtomicReference<ConcurrentLinkedQueue<ZNRecordUpdate>>();
  final ReadWriteLock _lock = new ReentrantReadWriteLock();
  
  class ZKPropertyTransferTask extends TimerTask
  {
    @Override
    public void run()
    {
      ConcurrentLinkedQueue<ZNRecordUpdate> updateQueue  = null;
      _lock.writeLock().lock();
      try
      {
        updateQueue = _dataQueueRef.getAndSet(new ConcurrentLinkedQueue<ZNRecordUpdate>());
      }
      finally
      {
        _lock.writeLock().unlock();
      }
      
      if(updateQueue != null)
      {
        List<String> paths = new ArrayList<String>();
        List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
        List<ZNRecord> vals = new ArrayList<ZNRecord>();
        for(ZNRecordUpdate holder : updateQueue)
        {
          paths.add(holder.getPath());
          updaters.add(holder.getZNRecordUpdater());
          vals.add(holder.getRecord());
        }
        if(paths.size() > 0)
        {
          _manager.getHelixDataAccessor().updateChildren(paths, updaters, BaseDataAccessor.Option.PERSISTENT);
        }
        LOG.info("Updating " + vals.size() + " records");
      }
      else
      {
        LOG.warn("null _dataQueueRef. Should be in the beginning only");
      }
    }
  }
  
  public ZKPropertyTransferServer(int localWebservicePort, HelixManager manager)
  {
    _localWebservicePort = localWebservicePort;
    _manager = manager;
    start();
  }
  
  public void enqueueData(ZNRecordUpdate e)
  {
    _lock.readLock().lock();
    try
    {
      _dataQueueRef.get().add(e);
    }
    finally
    {
      _lock.readLock().unlock();
    }
  }
  // TODO: how to do this async with restlet? Or do we use netty directly
  public static void sendZNRecordData(ZNRecord record, String path, PropertyType type, String serviceUrl)
  {
    ZNRecordUpdate update = new ZNRecordUpdate(path, type, record);
    Reference resourceRef = new Reference(serviceUrl);
    Request request = new Request(Method.PUT, resourceRef);
    
    ObjectMapper mapper = new ObjectMapper();
    StringWriter sw = new StringWriter();
    try
    {
      mapper.writeValue(sw, update);
    }
    catch (JsonGenerationException e)
    {
      LOG.error("",e);
    }
    catch (JsonMappingException e)
    {
      LOG.error("",e);
    }
    catch (IOException e)
    {
      LOG.error("",e);
    }

    request.setEntity(
        ZNRecordUpdateResource.UPDATEKEY + "=" + sw, MediaType.APPLICATION_ALL);
    Client client = new Client(Protocol.HTTP);
    Response response = client.handle(request);
    
    if(response.getStatus() != Status.SUCCESS_OK)
    {
      LOG.error("Status : " + response.getStatus());
    }
  }
  
  @Override
  public void run()
  {
    LOG.info("Cluster " + _manager.getClusterName() + " " + _manager.getInstanceName() 
        + " zkDataTransferServer starting...");
    Component component = new Component();
    component.getServers().add(Protocol.HTTP, _localWebservicePort);
    Context applicationContext = component.getContext().createChildContext();
    applicationContext.getAttributes().put(SERVER, this);
    applicationContext.getAttributes().put(PORT, "" + _localWebservicePort);
    ZkPropertyTransferApplication application = new ZkPropertyTransferApplication(
        applicationContext);
    // Attach the application to the component and start it
    component.getDefaultHost().attach(application);
    _timer = new Timer();
    _timer.schedule(new ZKPropertyTransferTask(), PERIOD , PERIOD);
    try
    {
      component.start();
    }
    catch (Exception e)
    {
      LOG.error("", e);
    }
    LOG.info("Cluster " + _manager.getClusterName() + " " + _manager.getInstanceName() 
        + " zkDataTransferServer started");
  }
  
  public void shutdown()
  {
    LOG.info("Cluster " + _manager.getClusterName() + " " + _manager.getInstanceName() 
        + " zkDataTransferServer shuting down");
    if(_timer != null)
    {
      _timer.cancel();
    }
    interrupt();
  }
}
