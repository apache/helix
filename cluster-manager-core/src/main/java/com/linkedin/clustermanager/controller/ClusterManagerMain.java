package com.linkedin.clustermanager.controller;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.tools.ClusterSetup;

public class ClusterManagerMain
{
  public static final String zkServerAddress = "zkSvr";
  public static final String cluster = "cluster";
  public static final String help = "help";

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions()
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

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(clusterOption);
    return options;
  }

  public static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + ClusterManagerMain.class.getName(),
        cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs)
      throws Exception
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try
    {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe)
    {
      System.err
          .println("CommandLineClient: failed to parse command-line options: "
              + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  public static void main(String[] args) throws Exception
  {
    // read the config
    // check if the this process is the master wait indefinitely
    // other approach is always process the events but when updating the zk
    // check if this is master.
    // This is difficult to get right
    // get the clusters to manage
    // for each cluster create a manager
    // add the respective listeners for each manager

    String clusterName = "test-cluster";
    String zkConnectString = "localhost:2181";
    if (args.length > 0)
    {
      CommandLine cmd = processCommandLineArgs(args);
      zkConnectString = cmd.getOptionValue(zkServerAddress);
      clusterName = cmd.getOptionValue(cluster);
    }
    // Espresso_driver.py will consume this
    System.out.println("Cluster manager started");
    // start the managers
    ClusterManager manager = ClusterManagerFactory
        .getZKBasedManagerForController(clusterName, zkConnectString);

    ClusterController controller = new ClusterController();
    manager.addConfigChangeListener(controller);
    manager.addLiveInstanceChangeListener(controller);
    manager.addIdealStateChangeListener(controller);
    manager.addExternalViewChangeListener(controller);
    // Message listener is not needed
    // manager.addMessageListener(controller);
    // currentstatechangelistener will be added by
    // liveInstanceChangeListener
    // manager.addCurrentStateChangeListener(controller);

    manager.connect();

    Thread.currentThread().join();
  }
}
