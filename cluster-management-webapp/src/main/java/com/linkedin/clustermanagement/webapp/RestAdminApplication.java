package com.linkedin.clustermanagement.webapp;

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

import com.linkedin.clustermanagement.webapp.resources.ClusterResource;
import com.linkedin.clustermanagement.webapp.resources.ClustersResource;
import com.linkedin.clustermanagement.webapp.resources.ExternalViewResource;
import com.linkedin.clustermanagement.webapp.resources.HostedEntitiesResource;
import com.linkedin.clustermanagement.webapp.resources.HostedEntityResource;
import com.linkedin.clustermanagement.webapp.resources.IdealStateResource;
import com.linkedin.clustermanagement.webapp.resources.InstanceResource;
import com.linkedin.clustermanagement.webapp.resources.InstancesResource;
import com.linkedin.clustermanager.tools.ClusterSetup;

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
    router.attach("/clusters/{clusterName}/hostedEntities", HostedEntitiesResource.class);
    router.attach("/clusters/{clusterName}/hostedEntities/{entityId}", HostedEntityResource.class);
    router.attach("/clusters/{clusterName}/instances", InstancesResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}", InstanceResource.class);
    router.attach("/clusters/{clusterName}/hostedEntities/{entityId}/idealState", IdealStateResource.class);
    router.attach("/clusters/{clusterName}/hostedEntities/{entityId}/externalView", ExternalViewResource.class);

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
