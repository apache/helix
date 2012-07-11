package com.linkedin.helix.controller.restlet;

import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.Router;

public class ZkPropertyTransferApplication extends Application
{
  public ZkPropertyTransferApplication()
  {
    super();
  }

  public ZkPropertyTransferApplication(Context context)
  {
    super(context);
  }
  
  @Override
  public Restlet createRoot()
  {
    Router router = new Router(getContext());
    router.attach("/ZNRecordUpdates", ZNRecordUpdateResource.class);
    return router;
  }
}