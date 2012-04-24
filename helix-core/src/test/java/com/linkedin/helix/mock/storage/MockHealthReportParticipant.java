/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.mock.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.healthcheck.HealthReportProvider;


public class MockHealthReportParticipant
{
  private static final Logger LOG = Logger.getLogger(MockHealthReportParticipant.class);
  public static final String zkServer = "zkSvr";
  public static final String cluster = "cluster";
  public static final String host = "host";
  public static final String port = "port";
  public static final String help = "help";

  static class MockHealthReportProvider extends HealthReportProvider
  {
    private final String _reportName = "MockRestQueryStats";
    private final Map<String, Map<String, String>> _mockHealthReport;

    public MockHealthReportProvider()
    {
      _mockHealthReport = new HashMap<String, Map<String, String>>();

      Map<String, String> reportMap = new HashMap<String, String>();
      _mockHealthReport.put("MockRestQueryStats@DBName=BizProfile", reportMap);

      reportMap.put("MeanMysqlLatency", "2.132700625");
      reportMap.put("95PercentileLatencyLucene", "108.40825525");
      reportMap.put("99PercentileLatencyMysql", "9.369827");
      reportMap.put("99PercentileLatencyServer", "167.714208");
      reportMap.put("95PercentileLatencyMysqlPool", "8.03621375");
      reportMap.put("95PercentileLatencyServer", "164.68374265");
      reportMap.put("MinLuceneLatency", "1.765908");
      reportMap.put("MaxServerLatency", "167.714208");
      reportMap.put("MeanLuceneLatency", "16.107599458333336");
      reportMap.put("CollectorName", "RestQueryStats");
      reportMap.put("MeanLucenePoolLatency", "8.120545333333332");
      reportMap.put("99PercentileLatencyLucenePool", "65.930564");
      reportMap.put("MinServerLatency", "0.425272");
      reportMap.put("IndexStoreMismatchCount", "0");
      reportMap.put("ErrorCount", "0");
      reportMap.put("MeanMysqlPoolLatency", "1.0704102916666667");
      reportMap.put("MinLucenePoolLatency", "0.008189");
      reportMap.put("MinMysqlLatency", "0.709691");
      reportMap.put("MaxMysqlPoolLatency", "8.606973");
      reportMap.put("99PercentileLatencyMysqlPool", "8.606973");
      reportMap.put("MinMysqlPoolLatency", "0.091883");
      reportMap.put("MaxLucenePoolLatency", "65.930564");
      reportMap.put("99PercentileLatencyLucene", "111.78799");
      reportMap.put("MaxMysqlLatency", "9.369827");
      reportMap.put("TimeStamp", "1332895048143");
      reportMap.put("MeanConcurrencyLevel", "1.9");
      reportMap.put("95PercentileLatencyMysql", "8.96594875");
      reportMap.put("QueryStartCount", "0");
      reportMap.put("95PercentileLatencyLucenePool", "63.518656500000006");
      reportMap.put("MeanServerLatency", "39.5451532");
      reportMap.put("MaxLuceneLatency", "111.78799");
      reportMap.put("QuerySuccessCount", "0");
    }

    @Override
    public Map<String, String> getRecentHealthReport()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void resetStats()
    {
      // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Map<String, String>> getRecentPartitionHealthReport()
    {
      // tweek: randomly change the last digit
      for (String key1 : _mockHealthReport.keySet())
      {
        Map<String, String> reportMap = _mockHealthReport.get(key1);
        for (String key2 : reportMap.keySet())
        {
          String value = reportMap.get(key2);
          String lastDigit = "" + new Random().nextInt(10);
          value = value.substring(0, value.length() - 1) + lastDigit;
          reportMap.put(key2, value);
        }
      }

      return _mockHealthReport;
    }

    @Override
    public String getReportName()
    {
      return _reportName;
    }
  }

  static class MockHealthReportJob implements MockJobIntf
  {

    @Override
    public void doPreConnectJob(HelixManager manager)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void doPostConnectJob(HelixManager manager)
    {
      // TODO Auto-generated method stub
      manager.getHealthReportCollector().addHealthReportProvider(new MockHealthReportProvider());
    }

  }

  // hack OptionBuilder is not thread safe
  @SuppressWarnings("static-access")
  synchronized private static Options constructCommandLineOptions()
  {
    Option helpOption = OptionBuilder.withLongOpt(help)
        .withDescription("Prints command-line options info").create();

    Option clusterOption = OptionBuilder.withLongOpt(cluster)
        .withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option hostOption = OptionBuilder.withLongOpt(host)
        .withDescription("Provide host name").create();
    hostOption.setArgs(1);
    hostOption.setRequired(true);
    hostOption.setArgName("Host name (Required)");

    Option portOption = OptionBuilder.withLongOpt(port)
        .withDescription("Provide host port").create();
    portOption.setArgs(1);
    portOption.setRequired(true);
    portOption.setArgName("Host port (Required)");

    Option zkServerOption = OptionBuilder.withLongOpt(zkServer)
      .withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("Zookeeper server address(Required)");

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(clusterOption);
    options.addOption(hostOption);
    options.addOption(portOption);
    options.addOption(zkServerOption);

    return options;
  }

  public static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + MockHealthReportParticipant.class.getName(), cliOptions);
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
      System.err
          .println("CommandLineClient: failed to parse command-line options: "
              + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  // NOT working for kill -9, working for kill -2/-15
  static class MockHealthReportParticipantShutdownHook extends Thread
  {
    final MockParticipant _participant;
    MockHealthReportParticipantShutdownHook(MockParticipant participant)
    {
      _participant = participant;
    }

    @Override
    public void run()
    {
      LOG.info("MockHealthReportParticipantShutdownHook invoked");
      _participant.stop();
    }
  }

  public static void main(String[] args) throws Exception
  {
    CommandLine cmd = processCommandLineArgs(args);
    String zkConnectStr = cmd.getOptionValue(zkServer);
    String clusterName = cmd.getOptionValue(cluster);
    String hostStr = cmd.getOptionValue(host);
    String portStr = cmd.getOptionValue(port);

    String instanceName = hostStr + "_" + portStr;

    MockParticipant participant = new MockParticipant(clusterName, instanceName, zkConnectStr,
                                                      null, new MockHealthReportJob());
    Runtime.getRuntime().addShutdownHook(new MockHealthReportParticipantShutdownHook(participant));

    // Espresso_driver.py will consume this
    System.out.println("MockHealthReportParticipant process started, instanceName: " + instanceName);

    participant.run();
  }
}
