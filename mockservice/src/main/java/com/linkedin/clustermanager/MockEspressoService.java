package com.linkedin.clustermanager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
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

import com.linkedin.clustermanager.tools.ClusterSetup;

public class MockEspressoService extends Application
{
  private static final Logger logger = Logger.getLogger(MockEspressoService.class);
	
  public static final String HELP = "help";
  public static final String CLUSTERNAME = "clusterName";
  public static final String ZKSERVERADDRESS = "zkSvr";
  public static final String PORT = "port";
  public static final int DEFAULT_PORT = 8100;
  protected static final String NODE_TYPE = "EspressoStorage";
  String ZK_ADDR = "localhost:9999";
  String CLUSTER_NAME = "";
  protected static final String INSTANCE_NAME = "localhost_1234";
  
  public static final String DATABASENAME = "database";
  public static final String TABLENAME = "table";
  public static final String RESOURCENAME = "resource";
  public static final String SUBRESOURCENAME = "subresource";
 // protected static final String CLUSTER_NAME = "MockCluster";
  
  public static EspressoStorageMockNode _mockNode;
  public MockEspressoService()
  {
    super();
  }

  public MockEspressoService(Context context)
  {
    super(context);
  }

  //TODO: probably need to format these responses to look more like Espresso
  public static String doGet(String key) {
	  String resp = _mockNode.doGet(key);
	  if (resp == null) {
		  return "404 NOT FOUND";
	  }
	  else {
		  return "200 "+resp;
	  }
  }
  
  public static void doPut(String key, char[] value) {
	  _mockNode.doPut(key, value);
  }
  
  @Override
  public Restlet createRoot()
  {
    Router router = new Router(getContext());
    router.attach("/get/{"+DATABASENAME+"}/{"+TABLENAME+"}/{"+RESOURCENAME+"}", GetResource.class); ///{"+SUBRESOURCENAME+"}", GetResource.class);
    //TODO: make subresource optional
    router.attach("/post/{"+DATABASENAME+"}/{"+TABLENAME+"}/{"+RESOURCENAME+"}", PostResource.class);
    

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
    helpFormatter.printHelp("java " + MockEspressoService.class.getName(), cliOptions);
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
    
    Option clusterOption = OptionBuilder.withLongOpt(CLUSTERNAME)
            .withDescription("Provide cluster name").create();
        clusterOption.setArgs(1);
        clusterOption.setRequired(true);
        clusterOption.setArgName("Cluster name(Required)");
    
    Option portOption = OptionBuilder.withLongOpt(PORT)
    .withDescription("Provide web service port").create();
    portOption.setArgs(1);
    portOption.setRequired(false);
    portOption.setArgName("web service port, default: "+ DEFAULT_PORT);
        
    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(clusterOption);
    options.addOption(portOption);
    
    return options;
  }
  
  public void processCommandLineArgs(String[] cliArgs) throws Exception
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
      System.err.println("MockEspressoService: failed to parse command-line options: "
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
    if (cmd.hasOption(ZKSERVERADDRESS)) {
    	ZK_ADDR = cmd.getOptionValue(ZKSERVERADDRESS);
    }
    if (cmd.hasOption(CLUSTERNAME)) {
    	CLUSTER_NAME = cmd.getOptionValue(CLUSTERNAME);
    	logger.debug("CLUSTER_NAME: "+CLUSTER_NAME);
    }
    // start web server with the zkServer address
    Component component = new Component();
    component.getServers().add(Protocol.HTTP, port);
    Context applicationContext = component.getContext().createChildContext();
    applicationContext.getAttributes().put(ZKSERVERADDRESS, cmd.getOptionValue(ZKSERVERADDRESS));
    applicationContext.getAttributes().put(CLUSTERNAME, cmd.getOptionValue(CLUSTERNAME));
    
    MockEspressoService application = new MockEspressoService(
        applicationContext); 
    // Attach the application to the component and start it
    component.getDefaultHost().attach(application);
    component.start();
  }
  
  public void run() {
	 
	  
	  CMConnector cm = null;
	  try {
		  logger.debug("xxx"+ZK_ADDR+"xxx");
		  cm = new CMConnector(CLUSTER_NAME, INSTANCE_NAME, ZK_ADDR); //, zkClient);
	  }
	  catch (Exception e) {
		  logger.error("Unable to initialize CMConnector: "+e);
		  e.printStackTrace();
		  System.exit(-1);
	  }
	  _mockNode = (EspressoStorageMockNode)MockNodeFactory.createMockNode(NODE_TYPE, cm);
	  if (_mockNode != null) {
		  _mockNode.run();
	  }
	  else {
		  logger.error("Unknown MockNode type "+NODE_TYPE);
		  System.exit(-1);
	  }
  }
  
  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
	  MockEspressoService service = new MockEspressoService();
	  service.processCommandLineArgs(args);
	  service.run();
	  
  }
}
