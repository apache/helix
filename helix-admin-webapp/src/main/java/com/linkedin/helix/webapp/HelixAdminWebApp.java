package com.linkedin.helix.webapp;

import org.apache.log4j.Logger;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;

import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;

public class HelixAdminWebApp
{
  public final Logger LOG = Logger.getLogger(HelixAdminWebApp.class);
  RestAdminApplication _adminApp = null;
  Component _component = null; 
  
  int _helixAdminPort;
  String _zkServerAddress;
  ZkClient _zkClient;
  
  public HelixAdminWebApp(String zkServerAddress, int adminPort)
  {
    _zkServerAddress = zkServerAddress;
    _helixAdminPort = adminPort;
  }
  
  public synchronized void start() throws Exception
  {
    LOG.info("helixAdminWebApp starting");
    if(_component == null)
    {
      _zkClient = new ZkClient(_zkServerAddress,  ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      _component =  new Component();
      _component.getServers().add(Protocol.HTTP, _helixAdminPort);
      Context applicationContext = _component.getContext().createChildContext();
      applicationContext.getAttributes().put(RestAdminApplication.ZKSERVERADDRESS, _zkServerAddress);
      applicationContext.getAttributes().put(RestAdminApplication.PORT, ""+_helixAdminPort);
      applicationContext.getAttributes().put(RestAdminApplication.ZKCLIENT, _zkClient);
      _adminApp = new RestAdminApplication(applicationContext); 
      // Attach the application to the component and start it
      _component.getDefaultHost().attach(_adminApp);
      _component.start();
    }
    LOG.info("helixAdminWebApp started on port " + _helixAdminPort);
  }
  
  public synchronized void stop() 
  {
    try
    {
      _component.stop();
    }
    catch(Exception e)
    {
      LOG.error("", e);
    }
    finally
    {
      if(_zkClient != null)
      {
        _zkClient.close();
      }
    }
  }
}
