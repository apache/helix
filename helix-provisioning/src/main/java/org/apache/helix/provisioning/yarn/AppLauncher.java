package org.apache.helix.provisioning.yarn;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * Main class to launch the job.
 * Gets the yaml file as the input.
 * Converts yaml file into ApplicationSpec.
 */
public class AppLauncher {
  private static final Log LOG = LogFactory.getLog(Client.class);

  private ApplicationSpec _applicationSpec;
  private YarnClient yarnClient;
  private ApplicationSpecFactory _applicationSpecFactory;
  private File _yamlConfigFile;

  private YarnConfiguration _conf;

  private File appMasterArchive;

  public AppLauncher(File appMasterArchive, ApplicationSpecFactory applicationSpecFactory,
      File yamlConfigFile) throws Exception {
    _applicationSpecFactory = applicationSpecFactory;
    _yamlConfigFile = yamlConfigFile;
    init();
  }

  private void init() throws Exception {
    _applicationSpec = _applicationSpecFactory.fromYaml(new FileInputStream(_yamlConfigFile));
    yarnClient = YarnClient.createYarnClient();
    _conf = new YarnConfiguration();
    yarnClient.init(_conf);
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
    ApplicationId appId = appContext.getApplicationId();
    String appName = _applicationSpec.getAppName();
    appContext.setApplicationName(appName);

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    LOG.info("Copy App archive file from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    FileSystem fs = FileSystem.get(_conf);

    // copy App Master package
    Path src = new Path(appMasterArchive.getAbsolutePath());
    String pathSuffix = appName + "/" + appId.getId() + "/" + "appmaster-pkg" + ".tar";
    Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
    fs.copyFromLocalFile(false, true, src, dst);
    FileStatus destStatus = fs.getFileStatus(dst);
    LocalResource amJarRsrc = setupLocalResource(dst, destStatus);
    localResources.put("appmaster-pkg", amJarRsrc);

    // copy component packages
    for (String serviceName : _applicationSpec.getServices()) {
      src = new Path(_applicationSpec.getServicePackage(serviceName));
      pathSuffix = appName + "/" + appId.getId() + "/" + serviceName + ".tar";
      dst = new Path(fs.getHomeDirectory(), pathSuffix);
      fs.copyFromLocalFile(false, true, src, dst);
      destStatus = fs.getFileStatus(dst);
      amJarRsrc = setupLocalResource(dst, destStatus);
      localResources.put(serviceName + "-pkg", amJarRsrc);
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
        new StringBuilder(Environment.CLASSPATH.$()).append(File.pathSeparatorChar).append("./*");
    StringBuilder appClassPathEnv = new StringBuilder();
    // put the jar files under the archive in the classpath
    try {
      final InputStream is = new FileInputStream(appMasterArchive);
      final TarArchiveInputStream debInputStream =
          (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
      TarArchiveEntry entry = null;
      while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
        if (entry.isFile()) {
          appClassPathEnv.append(File.pathSeparatorChar);
          appClassPathEnv.append("./app-pkg/" + entry.getName());
        }
      }
      debInputStream.close();

    } catch (Exception e) {
      LOG.error("Unable to read archive file:" + appMasterArchive, e);
    }
    classPathEnv.append(appClassPathEnv);
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
    System.out.println("classpath" + classPathEnv.toString());
    // Set the env variables to be setup in the env where the application master will be run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();
    env.put("app_pkg_path", fs.getHomeDirectory() + "/" + appName + "/" + appId.getId()
        + "/app-pkg.tar");
    env.put("appName", appName);
    env.put("appId", "" + appId.getId());
    env.put("CLASSPATH", classPathEnv.toString());
    env.put("appClasspath", appClassPathEnv.toString());
    env.put("containerParticipantMainClass",
        "org.apache.helix.provisioning.yarn.ParticipantLauncher");
    amContainer.setEnvironment(env);

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    int amMemory = 1024;
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
    // Set class name
    vargs.add(HelixYarnApplicationMasterMain.class.getCanonicalName());
    // Set params for Application Master
    //vargs.add("--num_containers " + String.valueOf(numContainers));

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
    int amPriority= 0;
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(amPriority);
    appContext.setPriority(pri);

    String amQueue="";
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue);

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    LOG.info("Submitting application to ASM");

    yarnClient.submitApplication(appContext);

    return true;
  }

  private LocalResource setupLocalResource(Path dst, FileStatus destStatus) {
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need the jar file to be untarred for now
    amJarRsrc.setType(LocalResourceType.ARCHIVE);
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

  /**
   * @return true if successfully completed, it will print status every X seconds
   */
  public boolean waitUntilDone() {
    while (true) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        break;
      }
    }
    return true;
  }

  /**
   * will take the input file and AppSpecFactory class name as input
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    File appMasterArchive = new File (args[0]);
    ApplicationSpecFactory applicationSpecFactory =
        (ApplicationSpecFactory) Class.forName(args[1]).newInstance();
    File yamlConfigFile = new File(args[2]);
    AppLauncher launcher = new AppLauncher(appMasterArchive, applicationSpecFactory, yamlConfigFile);
    launcher.launch();
    launcher.waitUntilDone();

  }

}
