package com.linkedin.helix.webapp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.resources.ClusterResource;
import com.linkedin.helix.webapp.resources.ClustersResource;
import com.linkedin.helix.webapp.resources.CurrentStateResource;
import com.linkedin.helix.webapp.resources.CurrentStatesResource;
import com.linkedin.helix.webapp.resources.ErrorResource;
import com.linkedin.helix.webapp.resources.ErrorsResource;
import com.linkedin.helix.webapp.resources.ExternalViewResource;
import com.linkedin.helix.webapp.resources.HostedResourceGroupResource;
import com.linkedin.helix.webapp.resources.HostedResourceGroupsResource;
import com.linkedin.helix.webapp.resources.IdealStateResource;
import com.linkedin.helix.webapp.resources.InstanceResource;
import com.linkedin.helix.webapp.resources.InstancesResource;
import com.linkedin.helix.webapp.resources.SchedulerTasksResource;
import com.linkedin.helix.webapp.resources.StateModelResource;
import com.linkedin.helix.webapp.resources.StateModelsResource;
import com.linkedin.helix.webapp.resources.StatusUpdateResource;
import com.linkedin.helix.webapp.resources.StatusUpdatesResource;

public class RestAdminApplication extends Application
{
  public static final String HELP = "help";
  public static final String ZKSERVERADDRESS = "zkSvr";
  public static final String PORT = "port";
  public static final int DEFAULT_PORT = 8100;
  
  public RestAdminApplication()
  {
    super();
  }

  public RestAdminApplication(Context context)
  {
    super(context);
  }

  @Override
  public Restlet createRoot()
  {
    Router router = new Router(getContext());
    router.attach("/clusters", ClustersResource.class);
    router.attach("/clusters/{clusterName}", ClusterResource.class);
    router.attach("/clusters/{clusterName}/resourceGroups", HostedResourceGroupsResource.class);
    router.attach("/clusters/{clusterName}/resourceGroups/{resourceName}", HostedResourceGroupResource.class);
    router.attach("/clusters/{clusterName}/instances", InstancesResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}", InstanceResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/currentState/{resourceName}", CurrentStateResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/statusUpdate/{resourceName}", StatusUpdateResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/errors/{resourceName}", ErrorResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/currentState", CurrentStatesResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/statusUpdate", StatusUpdatesResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/errors", ErrorsResource.class);
    router.attach("/clusters/{clusterName}/resourceGroups/{resourceName}/idealState", IdealStateResource.class);
    router.attach("/clusters/{clusterName}/resourceGroups/{resourceName}/externalView", ExternalViewResource.class);
    router.attach("/clusters/{clusterName}/StateModelDefs/{modelName}", StateModelResource.class);
    router.attach("/clusters/{clusterName}/StateModelDefs", StateModelsResource.class);
    router.attach("/clusters/{clusterName}/SchedulerTasks", SchedulerTasksResource.class);
    

    Restlet mainpage = new Restlet()
    {
      @Override
      public void handle(Request request, Response response)
      {
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
        response.setEntity(new StringRepresentation(stringBuilder.toString(),
            MediaType.TEXT_HTML));
      }
    };
    router.attach("", mainpage);
    return router;
  }
  
  public static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + RestAdminApplication.class.getName(), cliOptions);
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions()
  {
    Option helpOption = OptionBuilder.withLongOpt(HELP)
        .withDescription("Prints command-line options info").create();
    helpOption.setArgs(0);
    helpOption.setRequired(false);
    helpOption.setArgName("print help message");
    
    Option zkServerOption = OptionBuilder.withLongOpt(ZKSERVERADDRESS)
        .withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");
    
    Option portOption = OptionBuilder.withLongOpt(PORT)
    .withDescription("Provide web service port").create();
    portOption.setArgs(1);
    portOption.setRequired(false);
    portOption.setArgName("web service port, default: "+ DEFAULT_PORT);
        
    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(portOption);
    
    return options;
  }
  
  public static void processCommandLineArgs(String[] cliArgs) throws Exception
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try
    {
      cmd = cliParser.parse(cliOptions, cliArgs);
    } 
    catch (ParseException pe)
    {
      System.err.println("RestAdminApplication: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    int port = DEFAULT_PORT;
    if(cmd.hasOption(HELP))
    {
      printUsage(cliOptions);
      return;
    }
    else if(cmd.hasOption(PORT))
    {
      port = Integer.parseInt(cmd.getOptionValue(PORT));
    }
    // start web server with the zkServer address
    Component component = new Component();
    component.getServers().add(Protocol.HTTP, port);
    Context applicationContext = component.getContext().createChildContext();
    applicationContext.getAttributes().put(ZKSERVERADDRESS, cmd.getOptionValue(ZKSERVERADDRESS));

    RestAdminApplication application = new RestAdminApplication(
        applicationContext); 
    // Attach the application to the component and start it
    component.getDefaultHost().attach(application);
    component.start();
  }
  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
    processCommandLineArgs(args);
  }

}
