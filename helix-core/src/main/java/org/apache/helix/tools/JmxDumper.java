package org.apache.helix.tools;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.relation.MBeanServerNotificationFilter;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class JmxDumper implements NotificationListener {
  public static final String help = "help";
  public static final String domain = "domain";
  public static final String fields = "fields";
  public static final String pattern = "pattern";
  public static final String operations = "operations";
  public static final String period = "period";
  public static final String className = "className";
  public static final String outputFile = "outputFile";
  public static final String jmxUrl = "jmxUrl";
  public static final String sampleCount = "sampleCount";

  private static final Logger _logger = Logger.getLogger(JmxDumper.class);
  String _domain;
  MBeanServerConnection _mbeanServer;

  String _beanClassName;
  String _namePattern;
  int _samplePeriod;

  Map<ObjectName, ObjectName> _mbeanNames = new ConcurrentHashMap<ObjectName, ObjectName>();
  Timer _timer;

  String _outputFileName;

  List<String> _outputFields = new ArrayList<String>();
  Set<String> _operations = new HashSet<String>();
  PrintWriter _outputFile;
  int _samples = 0;
  int _targetSamples = -1;
  String _jmxUrl;

  public JmxDumper(String jmxService, String domain, String beanClassName, String namePattern,
      int samplePeriod, List<String> fields, List<String> operations, String outputfile,
      int sampleCount) throws Exception {
    _jmxUrl = jmxService;
    _domain = domain;
    _beanClassName = beanClassName;
    _samplePeriod = samplePeriod;
    _outputFields.addAll(fields);
    _operations.addAll(operations);
    _outputFileName = outputfile;
    _namePattern = namePattern;
    _targetSamples = sampleCount;

    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + _jmxUrl + "/jmxrmi");
    JMXConnector jmxc = JMXConnectorFactory.connect(url, null);

    _mbeanServer = jmxc.getMBeanServerConnection();
    MBeanServerNotificationFilter filter = new MBeanServerNotificationFilter();
    filter.enableAllObjectNames();
    _mbeanServer.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this, filter, null);
    init();
    _timer = new Timer(true);
    _timer.scheduleAtFixedRate(new SampleTask(), _samplePeriod, _samplePeriod);
  }

  class SampleTask extends TimerTask {
    @Override
    public void run() {
      List<ObjectName> errorMBeans = new ArrayList<ObjectName>();
      _logger.info("Sampling " + _mbeanNames.size() + " beans");
      for (ObjectName beanName : _mbeanNames.keySet()) {
        MBeanInfo info;
        try {
          info = _mbeanServer.getMBeanInfo(beanName);
        } catch (Exception e) {
          _logger.error(e.getMessage() + " removing it");
          errorMBeans.add(beanName);
          continue;
        }
        if (!info.getClassName().equals(_beanClassName)) {
          _logger.warn("Skip: className " + info.getClassName() + " expected : " + _beanClassName);
          continue;
        }
        StringBuffer line = new StringBuffer();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh:mm:ss:SSS");
        String date = dateFormat.format(new Date());
        line.append(date + " ");
        line.append(beanName.toString() + " ");

        MBeanAttributeInfo[] infos = info.getAttributes();
        Map<String, MBeanAttributeInfo> infoMap = new HashMap<String, MBeanAttributeInfo>();
        for (MBeanAttributeInfo infoItem : infos) {
          infoMap.put(infoItem.getName(), infoItem);
        }

        for (String outputField : _outputFields) {
          try {
            if (infoMap.containsKey(outputField)) {
              Object mbeanAttributeValue = _mbeanServer.getAttribute(beanName, outputField);
              line.append(mbeanAttributeValue.toString() + " ");
            } else {
              _logger.warn(outputField + " not found");
              line.append("null ");
            }
          } catch (Exception e) {
            _logger.error("Error:", e);
            line.append("null ");
            continue;
          }
        }
        MBeanOperationInfo[] operations = info.getOperations();
        Map<String, MBeanOperationInfo> opeMap = new HashMap<String, MBeanOperationInfo>();
        for (MBeanOperationInfo opeItem : operations) {
          opeMap.put(opeItem.getName(), opeItem);
        }

        for (String ope : _operations) {
          if (opeMap.containsKey(ope)) {
            try {
              _mbeanServer.invoke(beanName, ope, new Object[0], new String[0]);
              // System.out.println(ope+" invoked");
            } catch (Exception e) {
              _logger.error("Error:", e);
              continue;
            }
          }
        }
        _outputFile.println(line.toString());
        // System.out.println(line);
      }
      for (ObjectName deadBean : errorMBeans) {
        _mbeanNames.remove(deadBean);
      }

      _samples++;
      // System.out.println("samples:"+_samples);
      if (_samples == _targetSamples) {
        synchronized (JmxDumper.this) {
          _logger.info(_samples + " samples done, exiting...");
          JmxDumper.this.notifyAll();
        }
      }
    }
  }

  void init() throws Exception {
    try {
      Set<ObjectInstance> existingInstances =
          _mbeanServer.queryMBeans(new ObjectName(_namePattern), null);
      _logger.info("Total " + existingInstances.size() + " mbeans matched " + _namePattern);
      for (ObjectInstance instance : existingInstances) {
        if (instance.getClassName().equals(_beanClassName)) {
          _mbeanNames.put(instance.getObjectName(), instance.getObjectName());
          _logger.info("Sampling " + instance.getObjectName());
        }
      }
      FileWriter fos = new FileWriter(_outputFileName);
      System.out.println(_outputFileName);
      _outputFile = new PrintWriter(fos);
    } catch (Exception e) {
      _logger.error("fail to get all existing mbeans in " + _domain, e);
      throw e;
    }
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    MBeanServerNotification mbs = (MBeanServerNotification) notification;
    if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(mbs.getType())) {
      // System.out.println("Adding mbean " + mbs.getMBeanName());
      _logger.info("Adding mbean " + mbs.getMBeanName());
      if (mbs.getMBeanName().getDomain().equalsIgnoreCase(_domain)) {
        addMBean(mbs.getMBeanName());
      }
    } else if (MBeanServerNotification.UNREGISTRATION_NOTIFICATION.equals(mbs.getType())) {
      // System.out.println("Removing mbean " + mbs.getMBeanName());
      _logger.info("Removing mbean " + mbs.getMBeanName());
      if (mbs.getMBeanName().getDomain().equalsIgnoreCase(_domain)) {
        removeMBean(mbs.getMBeanName());
      }
    }
  }

  private void addMBean(ObjectName beanName) {
    _mbeanNames.put(beanName, beanName);
  }

  private void removeMBean(ObjectName beanName) {
    _mbeanNames.remove(beanName);
  }

  public static int processCommandLineArgs(String[] cliArgs) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try {
      cmd = cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      System.err.println("CommandLineClient: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    boolean ret = checkOptionArgsNumber(cmd.getOptions());
    if (ret == false) {
      printUsage(cliOptions);
      System.exit(1);
    }

    String portStr = cmd.getOptionValue(jmxUrl);
    // int portVal = Integer.parseInt(portStr);

    String periodStr = cmd.getOptionValue(period);
    int periodVal = Integer.parseInt(periodStr);

    String domainStr = cmd.getOptionValue(domain);
    String classNameStr = cmd.getOptionValue(className);
    String patternStr = cmd.getOptionValue(pattern);
    String fieldsStr = cmd.getOptionValue(fields);
    String operationsStr = cmd.getOptionValue(operations);
    String resultFile = cmd.getOptionValue(outputFile);
    String sampleCountStr = cmd.getOptionValue(sampleCount, "-1");
    int sampleCount = Integer.parseInt(sampleCountStr);

    List<String> fields = Arrays.asList(fieldsStr.split(","));
    List<String> operations = Arrays.asList(operationsStr.split(","));

    JmxDumper dumper = null;
    try {
      dumper =
          new JmxDumper(portStr, domainStr, classNameStr, patternStr, periodVal, fields,
              operations, resultFile, sampleCount);
      synchronized (dumper) {
        dumper.wait();
      }
    } finally {
      if (dumper != null) {
        dumper.flushFile();
      }
    }
    return 0;
  }

  private void flushFile() {
    if (_outputFile != null) {
      _outputFile.flush();
      _outputFile.close();
    }
  }

  private static boolean checkOptionArgsNumber(Option[] options) {
    for (Option option : options) {
      int argNb = option.getArgs();
      String[] args = option.getValues();
      if (argNb == 0) {
        if (args != null && args.length > 0) {
          System.err.println(option.getArgName() + " shall have " + argNb + " arguments (was "
              + Arrays.toString(args) + ")");
          return false;
        }
      } else {
        if (args == null || args.length != argNb) {
          System.err.println(option.getArgName() + " shall have " + argNb + " arguments (was "
              + Arrays.toString(args) + ")");
          return false;
        }
      }
    }
    return true;
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info")
            .create();
    Option domainOption =
        OptionBuilder.withLongOpt(domain).withDescription("Domain of the JMX bean").create();

    domainOption.setArgs(1);
    domainOption.setRequired(true);

    Option fieldsOption =
        OptionBuilder.withLongOpt(fields).withDescription("Fields of the JMX bean to sample")
            .create();
    fieldsOption.setArgs(1);
    fieldsOption.setRequired(false);

    Option operationOption =
        OptionBuilder.withLongOpt(operations).withDescription("Operation to invoke").create();
    operationOption.setArgs(1);
    operationOption.setRequired(true);

    Option periodOption =
        OptionBuilder.withLongOpt(period).withDescription("Sampling period in MS").create();
    periodOption.setArgs(1);
    periodOption.setRequired(false);

    Option classOption =
        OptionBuilder.withLongOpt(className).withDescription("Classname of the MBean").create();
    classOption.setArgs(1);
    classOption.setRequired(true);

    Option patternOption =
        OptionBuilder.withLongOpt(pattern).withDescription("pattern of the MBean").create();
    patternOption.setArgs(1);
    patternOption.setRequired(true);

    Option outputFileOption =
        OptionBuilder.withLongOpt(outputFile).withDescription("outputFileName").create();
    outputFileOption.setArgs(1);
    outputFileOption.setRequired(false);

    Option jmxUrlOption =
        OptionBuilder.withLongOpt(jmxUrl).withDescription("jmx port to connect to").create();
    jmxUrlOption.setArgs(1);
    jmxUrlOption.setRequired(true);

    Option sampleCountOption =
        OptionBuilder.withLongOpt(sampleCount).withDescription("# of samples to take").create();
    sampleCountOption.setArgs(1);
    sampleCountOption.setRequired(false);

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(domainOption);
    options.addOption(fieldsOption);
    options.addOption(operationOption);
    options.addOption(classOption);
    options.addOption(outputFileOption);
    options.addOption(jmxUrlOption);
    options.addOption(patternOption);
    options.addOption(periodOption);
    options.addOption(sampleCountOption);
    return options;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + JmxDumper.class.getName(), cliOptions);
  }

  public static void main(String[] args) throws Exception {
    /*
     * List<String> fields = Arrays.asList(new
     * String("AvgLatency,MaxLatency,MinLatency,PacketsReceived,PacketsSent").split(","));
     * List<String> operations = Arrays.asList(new String("resetCounters").split(","));
     * JmxDumper dumper = new JmxDumper(27961, "org.apache.zooKeeperService",
     * "org.apache.zookeeper.server.ConnectionBean",
     * "org.apache.ZooKeeperService:name0=*,name1=Connections,name2=*,name3=*", 1000, fields,
     * operations, "/tmp/1.csv");
     * Thread.currentThread().join();
     */
    int ret = processCommandLineArgs(args);
    System.exit(ret);
  }
}
