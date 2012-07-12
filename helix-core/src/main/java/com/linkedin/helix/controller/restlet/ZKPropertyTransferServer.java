package com.linkedin.helix.controller.restlet;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.ZNRecord;

public class ZKPropertyTransferServer
{
  public static final String PORT = "port";
  public static final String SERVER = "ZKPropertyTransferServer";
  public static int PERIOD = 10 * 1000;
  public static String RESTRESOURCENAME = "ZNRecordUpdates";
  private static Logger LOG = Logger.getLogger(ZKPropertyTransferServer.class);
  
  int _localWebservicePort;
  String _webserviceUrl;
  HelixManager _manager;
  Timer _timer;
  AtomicReference<ConcurrentHashMap<String, ZNRecordUpdate>> _dataCacheRef
    = new AtomicReference<ConcurrentHashMap<String, ZNRecordUpdate>>();
  final ReadWriteLock _lock = new ReentrantReadWriteLock();
  boolean _initialized = false;
  boolean _shutdownFlag = false;
  Component _component;
  
  class ZKPropertyTransferTask extends TimerTask
  {
    @Override
    public void run()
    {
      ConcurrentHashMap<String, ZNRecordUpdate> updateCache  = null;
      
      synchronized(_dataCacheRef)
      {
        updateCache = _dataCacheRef.getAndSet(new ConcurrentHashMap<String, ZNRecordUpdate>());
      }
      
      if(updateCache != null)
      {
        List<String> paths = new ArrayList<String>();
        List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>();
        List<ZNRecord> vals = new ArrayList<ZNRecord>();
        for(ZNRecordUpdate holder : updateCache.values())
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
  
  static ZKPropertyTransferServer _instance = new ZKPropertyTransferServer();
  
  private ZKPropertyTransferServer()
  {
    _dataCacheRef.getAndSet(new ConcurrentHashMap<String, ZNRecordUpdate>());
  }
  
  public boolean isInitialized()
  {
    return _initialized;
  }
  
  public static ZKPropertyTransferServer getInstance()
  {
    return _instance;
  }
  
  public void init(int localWebservicePort, HelixManager manager)
  {
    if(!_initialized)
    {
      _localWebservicePort = localWebservicePort;
      _manager = manager;
      startServer();
    }
    else
    {
      LOG.error("Already initialized with port " + _localWebservicePort);
    }
  }
  
  public String getWebserviceUrl()
  {
    if(!_initialized || _shutdownFlag)
    {
      LOG.error("inited:" + _initialized + " shutdownFlag:"+_shutdownFlag+" , return");
      return null;
    }
    
    return _webserviceUrl;
  }
  
  public void enqueueData(ZNRecordUpdate e)
  {
    if(!_initialized || _shutdownFlag)
    {
      LOG.error("inited:" + _initialized + " shutdownFlag:"+_shutdownFlag+" , return");
      return;
    }
    synchronized(_dataCacheRef)
    {
      if(_dataCacheRef.get().containsKey(e.getPath()))
      {
        ZNRecord oldVal = _dataCacheRef.get().get(e.getPath()).getRecord();
        oldVal = e.getZNRecordUpdater().update(oldVal);
        _dataCacheRef.get().get(e.getPath())._record = oldVal;
      }
      else
      {
        _dataCacheRef.get().put(e.getPath(), e);
      }
    }
  }
  
  public void startServer()
  {
    LOG.info("Cluster " + _manager.getClusterName() + " " + _manager.getInstanceName() 
        + " zkDataTransferServer starting...");
    _component = new Component();
    _component.getServers().add(Protocol.HTTP, _localWebservicePort);
    Context applicationContext = _component.getContext().createChildContext();
    applicationContext.getAttributes().put(SERVER, this);
    applicationContext.getAttributes().put(PORT, "" + _localWebservicePort);
    ZkPropertyTransferApplication application = new ZkPropertyTransferApplication(
        applicationContext);
    // Attach the application to the component and start it
    _component.getDefaultHost().attach(application);
    _timer = new Timer();
    _timer.schedule(new ZKPropertyTransferTask(), PERIOD , PERIOD);
    
    try
    {
      _webserviceUrl 
        = "http://" + InetAddress.getLocalHost().getCanonicalHostName() + ":" + _localWebservicePort 
              + "/" + RESTRESOURCENAME;
      _component.start();
      _initialized = true;
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
    try
    {
      _component.stop();
    }
    catch (Exception e)
    {
      LOG.error("", e);
    }
    
    _shutdownFlag = true;
  }
}
