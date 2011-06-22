package com.linkedin.clustermanager.mock.storage;

import org.apache.log4j.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.linkedin.clustermanager.impl.zk.ZKDataAccessor;
import com.linkedin.clustermanager.mock.consumer.ConsumerAdapter;
import com.linkedin.clustermanager.tools.ClusterSetup;

public class MockStorageProcess
{
    static Logger logger = Logger.getLogger(MockStorageProcess.class);

    public static final String zkServer = "zkSvr";
    public static final String cluster = "cluster";
    public static final String hostAddress = "host";
    public static final String hostPort = "port";
    public static final String relayCluster = "relayCluster";
    public static final String help = "help";

    private StorageAdapter storageAdapter;
    private ConsumerAdapter consumerAdapter;

    boolean put(Object key, Object val)
    {
        Integer partitionId = 1;
        storageAdapter.isMasterForPartition(partitionId);
        return true;
    }

    Object get(Object key)
    {
        Integer partitionId = 1;
        if (storageAdapter.isMasterForPartition(partitionId)
                || storageAdapter.isReplicaForPartition(partitionId))
        {
            return new String("val for " + key);
        }
        return null;
    }

    void start(String instanceName, String zkServerAddress, String clusterName,
            String relayClusterName) throws Exception
    {
        storageAdapter = new StorageAdapter(instanceName, zkServerAddress,
                clusterName, relayClusterName);
        storageAdapter.start();
    }

    @SuppressWarnings("static-access")
    private static Options constructCommandLineOptions()
    {
        Option helpOption = OptionBuilder.withLongOpt(help)
                .withDescription("Prints command-line options info").create();

        Option zkServerOption = OptionBuilder.withLongOpt(zkServer)
                .withDescription("Provide zookeeper address").create();
        zkServerOption.setArgs(1);
        zkServerOption.setRequired(true);
        zkServerOption.setArgName("ZookeeperServerAddress(Required)");

        Option clusterOption = OptionBuilder.withLongOpt(cluster)
                .withDescription("Provide cluster name").create();
        clusterOption.setArgs(1);
        clusterOption.setRequired(true);
        clusterOption.setArgName("Cluster name (Required)");

        Option hostOption = OptionBuilder.withLongOpt(hostAddress)
                .withDescription("Provide host name").create();
        hostOption.setArgs(1);
        hostOption.setRequired(true);
        hostOption.setArgName("Host name (Required)");

        Option portOption = OptionBuilder.withLongOpt(hostPort)
                .withDescription("Provide host port").create();
        portOption.setArgs(1);
        portOption.setRequired(true);
        portOption.setArgName("Host port (Required)");

        Option relayClusterOption = OptionBuilder.withLongOpt(relayCluster)
                .withDescription("Provide relay cluster name").create();
        relayClusterOption.setArgs(1);
        relayClusterOption.setRequired(true);
        relayClusterOption.setArgName("Relay cluster name (Required)");

        Options options = new Options();
        options.addOption(helpOption);
        options.addOption(zkServerOption);
        options.addOption(clusterOption);
        options.addOption(hostOption);
        options.addOption(portOption);
        options.addOption(relayClusterOption);
        return options;
    }

    public static void printUsage(Options cliOptions)
    {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("java " + ClusterSetup.class.getName(),
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
        }
        catch (ParseException pe)
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
        String clusterName = "storage-cluster";
        String relayClusterName = "relay-cluster";
        String zkServerAddress = "localhost:2181";
        String host = "localhost";
        int port = 8900;
        if (args.length > 0)
        {
            CommandLine cmd = processCommandLineArgs(args);
            zkServerAddress = cmd.getOptionValue(zkServer);
            clusterName = cmd.getOptionValue(cluster);
            relayClusterName = cmd.getOptionValue(relayCluster);
            host = cmd.getOptionValue(hostAddress);
            String portString = cmd.getOptionValue(hostPort);
            port = Integer.parseInt(portString);
        }
        // Espresso_driver.py will consume this
        System.out.println("Mock storage started");
        MockStorageProcess process = new MockStorageProcess();
        process.start(host + "_" + port, zkServerAddress, clusterName,
                relayClusterName);

        Thread.sleep(10000000);
    }
}
