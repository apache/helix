package org.apache.helix.provisioning.yarn;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.HelixConnection;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.provisioning.ApplicationSpec;
import org.apache.helix.provisioning.ApplicationSpecFactory;
import org.apache.helix.provisioning.HelixYarnUtil;
import org.apache.helix.provisioning.TaskConfig;

/**
 * Main class to launch the job.
 * Gets the yaml file as the input.
 * Converts yaml file into ApplicationSpec.
 */
public class AppLauncher {

  private static final Log LOG = LogFactory.getLog(AppLauncher.class);

  private ApplicationSpec _applicationSpec;
  private YarnClient yarnClient;
  private ApplicationSpecFactory _applicationSpecFactory;
  private File _yamlConfigFile;

  private YarnConfiguration _conf;

  private File appMasterArchive;

  private ApplicationId _appId;

  private AppMasterConfig _appMasterConfig;

  public AppLauncher(ApplicationSpecFactory applicationSpecFactory, File yamlConfigFile)
      throws Exception {
    _applicationSpecFactory = applicationSpecFactory;
    _yamlConfigFile = yamlConfigFile;
    init();
  }

  private void init() throws Exception {
    _applicationSpec = _applicationSpecFactory.fromYaml(new FileInputStream(_yamlConfigFile));
    _appMasterConfig = new AppMasterConfig();
    appMasterArchive = new File(_applicationSpec.getAppMasterPackage());
    yarnClient = YarnClient.createYarnClient();
    _conf = new YarnConfiguration();
    yarnClient.init(_conf);
  }

  public ApplicationSpec getApplicationSpec() {
    return _applicationSpec;
  }

  public boolean launch() throws Exception {
    LOG.info("Running Client");
    yarnClient.start();

    // Get a new application id
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    // TODO get min/max resource capabilities from RM and change memory ask if needed
    // If we do not have min/max, we may not be able to correctly request
    // the required resources from the RM for the app master
    // Memory ask has to be a multiple of min and less than max.
    // Dump out information about cluster capability as seen by the resource manager
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    // set the application name
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    _appId = appContext.getApplicationId();
    _appMasterConfig.setAppId(_appId.getId());
    String appName = _applicationSpec.getAppName();
    _appMasterConfig.setAppName(appName);
    _appMasterConfig.setApplicationSpecFactory(_applicationSpecFactory.getClass()
        .getCanonicalName());
    appContext.setApplicationName(appName);

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    LOG.info("Copy Application archive file from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    FileSystem fs = FileSystem.get(_conf);

    // get packages for each component packages
    Map<String, URI> packages = new HashMap<String, URI>();
    packages
        .put(AppMasterConfig.AppEnvironment.APP_MASTER_PKG.toString(), appMasterArchive.toURI());
    packages.put(AppMasterConfig.AppEnvironment.APP_SPEC_FILE.toString(), _yamlConfigFile.toURI());
    for (String serviceName : _applicationSpec.getServices()) {
      packages.put(serviceName, _applicationSpec.getServicePackage(serviceName));
    }
    Map<String, Path> hdfsDest = new HashMap<String, Path>();
    Map<String, String> classpathMap = new HashMap<String, String>();
    for (String name : packages.keySet()) {
      URI uri = packages.get(name);
      Path dst = copyToHDFS(fs, name, uri);
      hdfsDest.put(name, dst);
      String classpath = generateClasspathAfterExtraction(name, new File(uri));
      classpathMap.put(name, classpath);
      _appMasterConfig.setClasspath(name, classpath);
      String serviceMainClass = _applicationSpec.getServiceMainClass(name);
      if (serviceMainClass != null) {
        _appMasterConfig.setMainClass(name, serviceMainClass);
      }
    }

    // Get YAML files describing all workflows to immediately start
    Map<String, URI> workflowFiles = new HashMap<String, URI>();
    List<TaskConfig> taskConfigs = _applicationSpec.getTaskConfigs();
    if (taskConfigs != null) {
      for (TaskConfig taskConfig : taskConfigs) {
        URI configUri = taskConfig.getYamlURI();
        if (taskConfig.name != null && configUri != null) {
          workflowFiles.put(taskConfig.name, taskConfig.getYamlURI());
        }
      }
    }

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    LocalResource appMasterPkg =
        setupLocalResource(fs,
            hdfsDest.get(AppMasterConfig.AppEnvironment.APP_MASTER_PKG.toString()));
    LocalResource appSpecFile =
        setupLocalResource(fs,
            hdfsDest.get(AppMasterConfig.AppEnvironment.APP_SPEC_FILE.toString()));
    localResources.put(AppMasterConfig.AppEnvironment.APP_MASTER_PKG.toString(), appMasterPkg);
    localResources.put(AppMasterConfig.AppEnvironment.APP_SPEC_FILE.toString(), appSpecFile);
    for (String name : workflowFiles.keySet()) {
      URI uri = workflowFiles.get(name);
      Path dst = copyToHDFS(fs, name, uri);
      LocalResource taskLocalResource = setupLocalResource(fs, dst);
      localResources.put(AppMasterConfig.AppEnvironment.TASK_CONFIG_FILE.toString() + "_" + name,
          taskLocalResource);
    }

    // Set local resource info into app master container launch context
    amContainer.setLocalResources(localResources);

    // Set the necessary security tokens as needed
    // amContainer.setContainerTokens(containerToken);

    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv =
        new StringBuilder(Environment.CLASSPATH.$()).append(File.pathSeparatorChar).append("./*")
            .append(File.pathSeparatorChar);
    classPathEnv.append(classpathMap.get(AppMasterConfig.AppEnvironment.APP_MASTER_PKG.toString()));
    for (String c : _conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(File.pathSeparatorChar);
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");

    // add the runtime classpath needed for tests to work
    if (_conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':');
      classPathEnv.append(System.getProperty("java.class.path"));
    }
    LOG.info("\n\n Setting the classpath to launch AppMaster:\n\n");
    // Set the env variables to be setup in the env where the application master will be run
    Map<String, String> env = new HashMap<String, String>(_appMasterConfig.getEnv());
    env.put("CLASSPATH", classPathEnv.toString());

    amContainer.setEnvironment(env);

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master launch command");
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    int amMemory = 4096;
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
    // Set class name
    vargs.add(AppMasterLauncher.class.getCanonicalName());
    // Set params for Application Master
    // vargs.add("--num_containers " + String.valueOf(numContainers));

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    amContainer.setCommands(commands);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    appContext.setResource(capability);

    // Service data is a binary blob that can be passed to the application
    // Not needed in this scenario
    // amContainer.setServiceData(serviceData);

    // Setup security tokens
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = _conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
      }

      // For now, only getting tokens for the default file-system.
      final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }

    appContext.setAMContainerSpec(amContainer);

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    int amPriority = 0;
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(amPriority);
    appContext.setPriority(pri);

    String amQueue = "default";
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue);

    LOG.info("Submitting application to YARN Resource Manager");

    ApplicationId applicationId = yarnClient.submitApplication(appContext);

    LOG.info("Submitted application with applicationId:" + applicationId);

    return true;
  }

  /**
   * Generates the classpath after the archive file gets extracted under 'serviceName' folder
   * @param serviceName
   * @param archiveFile
   * @return
   */
  private String generateClasspathAfterExtraction(String serviceName, File archiveFile) {
    if (!isArchive(archiveFile.getAbsolutePath())) {
      return "./";
    }
    StringBuilder classpath = new StringBuilder();
    // put the jar files under the archive in the classpath
    try {
      final InputStream is = new FileInputStream(archiveFile);
      final TarArchiveInputStream debInputStream =
          (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
      TarArchiveEntry entry = null;
      while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
        if (entry.isFile()) {
          classpath.append(File.pathSeparatorChar);
          classpath.append("./" + serviceName + "/" + entry.getName());
        }
      }
      debInputStream.close();

    } catch (Exception e) {
      LOG.error("Unable to read archive file:" + archiveFile, e);
    }
    return classpath.toString();
  }

  private Path copyToHDFS(FileSystem fs, String name, URI uri) throws Exception {
    // will throw exception if the file name is without extension
    String extension = uri.getPath().substring(uri.getPath().lastIndexOf(".") + 1);
    String pathSuffix =
        _applicationSpec.getAppName() + "/" + _appId.getId() + "/" + name + "." + extension;
    Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
    Path src = new Path(uri);
    fs.copyFromLocalFile(false, true, src, dst);
    return dst;
  }

  private LocalResource setupLocalResource(FileSystem fs, Path dst) throws Exception {
    URI uri = dst.toUri();
    String extension = uri.getPath().substring(uri.getPath().lastIndexOf(".") + 1);
    FileStatus destStatus = fs.getFileStatus(dst);
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need the jar file to be untarred for now
    if (isArchive(extension)) {
      amJarRsrc.setType(LocalResourceType.ARCHIVE);
    } else {
      amJarRsrc.setType(LocalResourceType.FILE);
    }
    // Set visibility of the resource
    // Setting to most private option
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
    // Set timestamp and length of file so that the framework
    // can do basic sanity checks for the local resource
    // after it has been copied over to ensure it is the same
    // resource the client intended to use with the application
    amJarRsrc.setTimestamp(destStatus.getModificationTime());
    amJarRsrc.setSize(destStatus.getLen());
    return amJarRsrc;
  }

  private boolean isArchive(String path) {
    return path.endsWith("tar") || path.endsWith("gz") || path.endsWith("tar.gz")
        || path.endsWith("zip");
  }

  public HelixConnection pollForConnection() {
    String prevReport = "";
    HelixConnection connection = null;

    while (true) {
      try {
        // Get application report for the appId we are interested in
        ApplicationReport report = yarnClient.getApplicationReport(_appId);

        String reportMessage = generateReport(report);
        if (!reportMessage.equals(prevReport)) {
          LOG.info(reportMessage);
        }
        YarnApplicationState state = report.getYarnApplicationState();
        if (YarnApplicationState.RUNNING == state) {
          if (connection == null) {
            String hostName = null;
            int ind = report.getHost().indexOf('/');
            if (ind > -1) {
              hostName = report.getHost().substring(ind + 1);
            } else {
              hostName = report.getHost();
            }
            connection = new ZkHelixConnection(hostName + ":2181");

            try {
              connection.connect();
            } catch (Exception e) {
              LOG.warn("AppMaster started but not yet initialized");
              connection = null;
            }
          }
          if (connection.isConnected()) {
            return connection;
          }
        }
        prevReport = reportMessage;
        Thread.sleep(10000);
      } catch (Exception e) {
        LOG.error("Exception while getting info", e);
        break;
      }
    }
    return null;
  }

  public ApplicationReport getApplicationReport() {
    try {
      return yarnClient.getApplicationReport(_appId);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * @return true if successfully completed, it will print status every X seconds
   */
  public boolean waitUntilDone() {
    String prevReport = "";
    HelixConnection connection = null;

    while (true) {
      try {
        // Get application report for the appId we are interested in
        ApplicationReport report = yarnClient.getApplicationReport(_appId);

        String reportMessage = generateReport(report);
        if (!reportMessage.equals(prevReport)) {
          LOG.info(reportMessage);
        }
        YarnApplicationState state = report.getYarnApplicationState();
        FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
        if (YarnApplicationState.FINISHED == state) {
          if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
            LOG.info("Application has completed successfully. Breaking monitoring loop");
            return true;
          } else {
            LOG.info("Application did finished unsuccessfully." + " YarnState=" + state.toString()
                + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
            return false;
          }
        } else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
          LOG.info("Application did not finish." + " YarnState=" + state.toString()
              + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
          return false;
        }
        if (YarnApplicationState.RUNNING == state) {
          if (connection == null) {
            String hostName = null;
            int ind = report.getHost().indexOf('/');
            if (ind > -1) {
              hostName = report.getHost().substring(ind + 1);
            } else {
              hostName = report.getHost();
            }
            connection = new ZkHelixConnection(hostName + ":2181");

            try {
              connection.connect();
            } catch (Exception e) {
              LOG.warn("AppMaster started but not yet initialized");
              connection = null;
            }
          }
          if (connection.isConnected()) {
            AppStatusReportGenerator generator = new AppStatusReportGenerator();
            ClusterId clusterId = ClusterId.from(_applicationSpec.getAppName());
            String generateReport = generator.generateReport(connection, clusterId);
            LOG.info(generateReport);
          }
        }
        prevReport = reportMessage;
        Thread.sleep(10000);
      } catch (Exception e) {
        LOG.error("Exception while getting info", e);
        break;
      }
    }
    return true;
  }

  /**
   * TODO: kill the app only in dev mode. In prod, its ok for the app to continue running if the
   * launcher dies after launching
   */

  private String generateReport(ApplicationReport report) {
    return "Got application report from ASM for" + ", appId=" + _appId.getId()
        + ", clientToAMToken=" + report.getClientToAMToken() + ", appDiagnostics="
        + report.getDiagnostics() + ", appMasterHost=" + report.getHost() + ", appQueue="
        + report.getQueue() + ", appMasterRpcPort=" + report.getRpcPort() + ", appStartTime="
        + report.getStartTime() + ", yarnAppState=" + report.getYarnApplicationState().toString()
        + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
        + ", appTrackingUrl=" + report.getTrackingUrl() + ", appUser=" + report.getUser();
  }

  public void cleanup() {
    LOG.info("Cleaning up");
    try {
      ApplicationReport applicationReport = yarnClient.getApplicationReport(_appId);
      LOG.info("Killing application:" + _appId + " \n Application report"
          + generateReport(applicationReport));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Launches the application on a YARN cluster. Once launched, it will display (periodically) the
   * status of the containers in the application.
   * @param args app_spec_provider and app_config_spec
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    Options opts = new Options();
    opts.addOption(new Option("app_spec_provider", true,
        "Application Spec Factory Class that will parse the app_config_spec file"));
    opts.addOption(new Option("app_config_spec", true,
        "YAML config file that provides the app specifications"));
    CommandLine cliParser = new GnuParser().parse(opts, args);
    String appSpecFactoryClass = cliParser.getOptionValue("app_spec_provider");
    String yamlConfigFileName = cliParser.getOptionValue("app_config_spec");

    ApplicationSpecFactory applicationSpecFactory =
        HelixYarnUtil.createInstance(appSpecFactoryClass);
    File yamlConfigFile = new File(yamlConfigFileName);
    if (!yamlConfigFile.exists()) {
      throw new IllegalArgumentException("YAML app_config_spec file: '" + yamlConfigFileName
          + "' does not exist");
    }
    final AppLauncher launcher = new AppLauncher(applicationSpecFactory, yamlConfigFile);
    launcher.launch();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        launcher.cleanup();
      }
    }));
    launcher.waitUntilDone();
  }

}
