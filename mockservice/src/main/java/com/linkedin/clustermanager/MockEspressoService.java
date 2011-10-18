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
  protected static final String INSTANCE_NAME = "localhost_1234";
  
  public static final String DATABASENAME = "database";
  public static final String TABLENAME = "table";
  public static final String RESOURCENAME = "resource";
  public static final String SUBRESOURCENAME = "subresource";
  public static final String STOPSERVICECOMMAND = "stopservice";
  
  public static final String CONTEXT_MOCK_NODE_NAME = "mocknode";
  public static final String COMPONENT_NAME = "component";
  
  Context _applicationContext;
  static int _serverPort;
  static String _zkAddr = "localhost:9999";
  static String _clusterName = "";
  public CMConnector _connector;
  public EspressoStorageMockNode _mockNode;
  static Context _context;
  static Component _component;

  public MockEspressoService(Context context)
  {
    super(_context);
    _connector = null;
	  try {
		  _connector = new CMConnector(_clusterName, INSTANCE_NAME, _zkAddr); //, zkClient);
	  }
	  catch (Exception e) {
		  logger.error("Unable to initialize CMConnector: "+e);
		  e.printStackTrace();
		  System.exit(-1);
	  }
    _mockNode = (EspressoStorageMockNode)MockNodeFactory.createMockNode(NODE_TYPE, _connector);
    context.getAttributes().put(CONTEXT_MOCK_NODE_NAME, (Object)_mockNode);
  }
 
  @Override
  public Restlet createRoot()
  {
	Router router = new Router(_context);

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
    
    if (_mockNode == null) {
    	logger.debug("_mockNode in createRoot is null");
    }
    router.attach("", mainpage);
    
    //Espresso handlers
    router.attach("/{"+DATABASENAME+"}/{"+TABLENAME+"}/{"+RESOURCENAME+"}", EspressoResource.class);
    router.attach("/{"+DATABASENAME+"}/{"+TABLENAME+"}/{"+RESOURCENAME+"}/{"+SUBRESOURCENAME+"}", EspressoResource.class);
    
    //Admin handlers
    router.attach("/{"+STOPSERVICECOMMAND+"}", StopServiceResource.class);
    
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
      System.err.println("MockEspressoService: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    _serverPort = DEFAULT_PORT;
    if(cmd.hasOption(HELP))
    {
      printUsage(cliOptions);
      return;
    }
    else if(cmd.hasOption(PORT))
    {
      _serverPort = Integer.parseInt(cmd.getOptionValue(PORT));
    }
    if (cmd.hasOption(ZKSERVERADDRESS)) {
    	_zkAddr = cmd.getOptionValue(ZKSERVERADDRESS);
    }
    if (cmd.hasOption(CLUSTERNAME)) {
    	_clusterName = cmd.getOptionValue(CLUSTERNAME);
    	logger.debug("_clusterName: "+_clusterName);
    }
  }
  
  public void run() throws Exception {
	 
	  logger.debug("Start of mock service run");
	  
	
	  if (_mockNode == null) {
		  logger.debug("_mockNode null");
	  }
	  else {
		  logger.debug("_mockNode not null");
	  }
	  if (_mockNode != null) {
		  // start web server with the zkServer address
		  _component = new Component();
		  _component.getServers().add(Protocol.HTTP, _serverPort);
		  // Attach the application to the component and start it
		  _component.getDefaultHost().attach(this); //(application);
		  _context.getAttributes().put(COMPONENT_NAME, (Object)_component);
		  _component.start();
		  //start mock espresso node
		  _mockNode.run();
	  }
	  else {
		  logger.error("Unknown MockNode type "+NODE_TYPE);
		  System.exit(-1);
	  }
	  logger.debug("mock service done");
  }
  
  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
	  processCommandLineArgs(args);
	  _context = new Context();
	  MockEspressoService service = new MockEspressoService(_context);
	  service.run();
	  
  }
}
