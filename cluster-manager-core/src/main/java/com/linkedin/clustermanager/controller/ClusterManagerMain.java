package com.linkedin.clustermanager.controller;

/**
 * start cluster manager controller
 * cluster manager controller has two modes:
 * 1) stand-alone mode: in this mode each controller gets a list of clusters 
 *  and competes via leader election to become the controller for any of the clusters.
 *  if a controller fails to become the leader of a given cluster, it remains as a standby
 *  and re-does the leader election when the current leader fails
 *  
 * 2) distributed mode: in this mode each controller first joins as participant into 
 *   a special CONTROLLER_CLUSTER. Leader election happens in this special
 *   cluster. The one that becomes the leader controls all controllers (including itself
 *   to become leaders of other clusters. 
 */

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.monitoring.mbeans.ClusterStatusMonitor;
import com.linkedin.clustermanager.participant.DistClusterControllerStateModel;
import com.linkedin.clustermanager.participant.DistClusterControllerStateModelFactory;
import com.linkedin.clustermanager.participant.StateMachineEngine;

public class ClusterManagerMain
{
  public static final String zkServerAddress = "zkSvr";
  public static final String cluster = "cluster";
  public static final String help = "help";
  public static final String mode = "mode";
  public static final String name = "controllerName";
  public static final String STANDALONE = "STANDALONE";
  public static final String DISTRIBUTED = "DISTRIBUTED";
  private static final Logger logger = Logger.getLogger(ClusterManagerMain.class);

  // hack: OptionalBuilder is not thread safe
  @SuppressWarnings("static-access")
  synchronized private static Options constructCommandLineOptions()
  {
    Option helpOption = OptionBuilder.withLongOpt(help)
        .withDescription("Prints command-line options info").create();

    Option zkServerOption = OptionBuilder.withLongOpt(zkServerAddress)
        .withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option clusterOption = OptionBuilder.withLongOpt(cluster)
        .withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option modeOption = OptionBuilder.withLongOpt(mode)
        .withDescription("Provide cluster controller mode (Optional): STANDALONE (default) or DISTRIBUTED").create();
    modeOption.setArgs(1);
    modeOption.setRequired(false);
    modeOption.setArgName("Cluster controller mode (Optional)");

    Option controllerNameOption = OptionBuilder.withLongOpt(name)
        .withDescription("Provide cluster controller name (Optional)").create();
    controllerNameOption.setArgs(1);
    controllerNameOption.setRequired(false);
    controllerNameOption.setArgName("Cluster controller name (Optional)");
    
    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(clusterOption);
    options.addOption(modeOption);
    options.addOption(controllerNameOption);
    
    return options;
  }

  public static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + ClusterManagerMain.class.getName(), cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs)
  throws Exception
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();

    try
    {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe)
    {
      logger.error("fail to parse command-line options:" + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  public static void addListenersToController(ClusterManager manager, 
     GenericClusterController controller)
  {
    try
    {
      manager.addConfigChangeListener(controller);
      manager.addLiveInstanceChangeListener(controller);
      manager.addIdealStateChangeListener(controller);
      manager.addExternalViewChangeListener(controller);
    
      ClusterStatusMonitor monitor = new ClusterStatusMonitor(manager.getClusterName(), 
              manager.getDataAccessor().getClusterPropertyList(ClusterPropertyType.CONFIGS).size());
      manager.addLiveInstanceChangeListener(monitor);
      manager.addExternalViewChangeListener(monitor);
    }
    catch (Exception e)
    {
      logger.error("Error when creating ClusterManagerContollerMonitor", e);
      e.printStackTrace();
    }
  }
  
  public static ClusterManager startClusterManagerMain(final String zkConnectString, 
       final String clusterName, final String controllerName, final String controllerMode)
  {
    return startClusterManagerMain(zkConnectString, clusterName, controllerName, 
                            controllerMode, null);
  }
                                                  
  public static ClusterManager startClusterManagerMain(final String zkConnectString, 
       final String clusterName, final String controllerName, final String controllerMode, 
       final ZkClient zkClient)
  {
    ClusterManager manager = null;
    try
    {
      if (controllerMode.equalsIgnoreCase(STANDALONE))
      {
        manager = ClusterManagerFactory.getZKBasedManagerForController(clusterName, 
                   controllerName, zkConnectString, zkClient);
      }
      else if (controllerMode.equalsIgnoreCase(DISTRIBUTED))
      {
        manager = ClusterManagerFactory.getZKBasedManagerForControllerParticipant(clusterName, 
                   controllerName, zkConnectString);
      
        DistClusterControllerStateModelFactory stateModelFactory 
           = new DistClusterControllerStateModelFactory(zkConnectString);
        StateMachineEngine<DistClusterControllerStateModel> genericStateMachineHandler 
           = new StateMachineEngine<DistClusterControllerStateModel>(stateModelFactory);
        manager.addMessageListener(genericStateMachineHandler, controllerName);
      }
      else
      {
        logger.error("cluster controller mode:" + controllerMode + " NOT supported");
        // throw new IllegalArgumentException("Unsupported cluster controller mode:" + controllerMode);
      }
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return manager;
  }
  
  public static void main(String[] args) throws InterruptedException, Exception
  {
    // read the config
    // check if the this process is the master wait indefinitely
    // other approach is always process the events but when updating the zk
    // check if this is master.
    // This is difficult to get right
    // get the clusters to manage
    // for each cluster create a manager
    // add the respective listeners for each manager
    CommandLine cmd = processCommandLineArgs(args);
    String zkConnectString = cmd.getOptionValue(zkServerAddress);
    String clusterName = cmd.getOptionValue(cluster);
    String controllerMode = STANDALONE;
    String controllerName = null;
    if (cmd.hasOption(mode))
    {
      controllerMode = cmd.getOptionValue(mode);
    }
    
    if (controllerMode.equalsIgnoreCase(DISTRIBUTED) && !cmd.hasOption(name))
    {
      throw new IllegalArgumentException("A unique cluster controller name is required in DISTRIBUTED mode");
    }
    
    controllerName = cmd.getOptionValue(name);
    
    // Espresso_driver.py will consume this
    logger.info("Cluster manager started, zkServer: " + zkConnectString + 
        ", clusterName:" + clusterName + ", controllerName:" + controllerName + 
        ", mode:" + controllerMode);
    
    ClusterManager manager = startClusterManagerMain(zkConnectString, clusterName, 
                              controllerName, controllerMode);
    
    try
    {
      Thread.currentThread().join();
    }
    catch (InterruptedException e)
    {
      ClusterManagerFactory.disconnectManagers(controllerName);
      logger.info("thread:" + Thread.currentThread().getName() + " interrupted");
    }
  }
}
