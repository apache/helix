package com.linkedin.clustermanagement.webapp;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Request;
import org.restlet.Restlet;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.resource.StringRepresentation;
import org.restlet.Router;

import org.restlet.data.Response;

import com.linkedin.clustermanagement.webapp.resources.ClusterResource;
import com.linkedin.clustermanagement.webapp.resources.ClustersResource;
import com.linkedin.clustermanagement.webapp.resources.HostedEntitiesResource;
import com.linkedin.clustermanagement.webapp.resources.HostedEntityResource;
import com.linkedin.clustermanagement.webapp.resources.InstanceResource;
import com.linkedin.clustermanagement.webapp.resources.InstancesResource;



public class RestAdminApplication extends Application
 {
  public RestAdminApplication() {
    super();
  }

  public RestAdminApplication(Context context) {
    super(context);
  }

  @Override
  public Restlet createRoot() {
    Router router = new Router(getContext());
    router.attach("/clusters", ClustersResource.class);
    router.attach("/clusters/{clusterName}", ClusterResource.class);
    router.attach("/clusters/{clusterName}/hostedEntities", HostedEntitiesResource.class);
    router.attach("/clusters/{clusterName}/hostedEntities/{entityId}", HostedEntityResource.class);
    router.attach("/clusters/{clusterName}/instances", InstancesResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}", InstanceResource.class);
    
    Restlet mainpage = new Restlet() {
      @Override
      public void handle(Request request, Response response) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("<html>");
        stringBuilder
            .append("<head><title>Restlet Cluster Management page</title></head>");
        stringBuilder.append("<body bgcolor=white>");
        stringBuilder.append("<table border=\"0\">");
        stringBuilder.append("<tr>");
        stringBuilder.append("<td>");
        stringBuilder.append("<h1>Rest cluster management interface V1</h1>");
        stringBuilder.append("</td>");
        stringBuilder.append("</tr>");
        stringBuilder.append("</table>");
        stringBuilder.append("</body>");
        stringBuilder.append("</html>");
        response.setEntity(new StringRepresentation(stringBuilder
            .toString(), MediaType.TEXT_HTML));
      }
    };
    router.attach("", mainpage);
    return router;
  }
  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    Component component = new Component();
    component.getServers().add(Protocol.HTTP, 8100);
    component.getContext().getAttributes().put("zkServer", "localhost:2188");
    System.out.println("server started");
    RestAdminApplication application = new RestAdminApplication(
        component.getContext()); // Attach the application to the
                      // component and start it
                       component.getDefaultHost().attach(application);
                     component.start();
  }

}
