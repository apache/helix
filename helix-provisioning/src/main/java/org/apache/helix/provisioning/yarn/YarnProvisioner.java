package org.apache.helix.provisioning.yarn;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.config.ContainerConfig;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.helix.controller.provisioner.Provisioner;
import org.apache.helix.controller.provisioner.TargetProviderResponse;

import com.google.common.collect.Lists;

public class YarnProvisioner implements Provisioner {

  private static final Log LOG = LogFactory.getLog(YarnProvisioner.class);
  static GenericApplicationMaster applicationMaster;
  Map<ContainerId, Container> allocatedContainersMap = new HashMap<ContainerId, Container>();
  int DEFAULT_CONTAINER = 4;

  @Override
  public ContainerId allocateContainer(ContainerSpec spec) {
    ContainerRequest containerAsk = setupContainerAskForRM(spec);
    Future<ContainerAskResponse> requestNewContainer =
        applicationMaster.acquireContainer(containerAsk);
    ContainerAskResponse containerAskResponse;
    try {
      containerAskResponse = requestNewContainer.get();
      ContainerId helixContainerId =
          ContainerId.from(containerAskResponse.getContainer().getId().toString());
      allocatedContainersMap.put(helixContainerId, containerAskResponse.getContainer());
      return helixContainerId;
    } catch (Exception e) {
      LOG.error("Exception in allocateContainer for spec:" + spec, e);
    }
    return null;
  }

  @Override
  public boolean deallocateContainer(ContainerId containerId) {
    Future<ContainerReleaseResponse> releaseContainer =
        applicationMaster.releaseContainer(allocatedContainersMap.get(containerId));
    try {
      releaseContainer.get();
      return true;
    } catch (Exception e) {
      LOG.error("Exception in deallocateContainer containerId:" + containerId, e);
    }
    return false;
  }

  @Override
  public boolean startContainer(ContainerId containerId) {
    Container container = allocatedContainersMap.get(containerId);
    try {
      Future<ContainerLaunchResponse> launchContainer =
          applicationMaster.launchContainer(container, createLaunchContext(containerId));
      ContainerLaunchResponse containerLaunchResponse = launchContainer.get();
      return true;
    } catch (Exception e) {
      LOG.error("Exception while starting container containerId:" + containerId, e);
    }
    return false;
  }

  private ContainerLaunchContext createLaunchContext(ContainerId containerId) throws Exception {

    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    Map<String, String> envs = System.getenv();
    String appName = envs.get("appName");
    String appId = envs.get("appId");
    String appClasspath = envs.get("appClasspath");
    String containerParticipantMainClass = envs.get("containerParticipantMainClass");
    String zkAddress = envs.get(Environment.NM_HOST.name()) + ":2181";

    // set the localresources needed to launch container
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
    YarnConfiguration conf = new YarnConfiguration();
    FileSystem fs;
    fs = FileSystem.get(conf);
    String pathSuffix = appName + "/" + appId + "/app-pkg.tar";
    Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
    FileStatus destStatus = fs.getFileStatus(dst);

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
    localResources.put("app-pkg", amJarRsrc);

    // Set local resource info into app master container launch context
    amContainer.setLocalResources(localResources);

    // Set the necessary security tokens as needed
    // amContainer.setContainerTokens(containerToken);

    // Set the env variables to be setup in the env where the application master will be run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();
    env.put("app-pkg-path", dst.getName());
    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv =
        new StringBuilder(Environment.CLASSPATH.$()).append(File.pathSeparatorChar).append("./*");
    classPathEnv.append(File.pathSeparatorChar);
    classPathEnv.append(appClasspath);

    for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(File.pathSeparatorChar);
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");

    // add the runtime classpath needed for tests to work
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':');
      classPathEnv.append(System.getProperty("java.class.path"));
    }
    System.out.println("classoath" + classPathEnv.toString());
    env.put("CLASSPATH", classPathEnv.toString());

    amContainer.setEnvironment(env);

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + 1024 + "m");
    // Set class name
    vargs.add(containerParticipantMainClass);
    // Set params for container participant
    vargs.add("--zk_address " + zkAddress);
    vargs.add("--participantId " + containerId.stringify());

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/ContainerParticipant.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/ContainerParticipant.stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    amContainer.setCommands(commands);
    return amContainer;
  }

  @Override
  public boolean stopContainer(ContainerId containerId) {
    Container container = allocatedContainersMap.get(containerId);
    Future<ContainerStopResponse> stopContainer = applicationMaster.stopContainer(container);
    try {
      ContainerStopResponse containerStopResponse = stopContainer.get();
      return true;
    } catch (Exception e) {
      LOG.error("Exception while stopping container containerId:" + containerId, e);
    }
    return false;
  }

  @Override
  public ContainerState getContainerState(ContainerId containerId) {
    return null;
  }

  @Override
  public void init(HelixManager helixManager) {

  }

  @Override
  public TargetProviderResponse evaluateExistingContainers(Cluster cluster, ResourceId resourceId,
      Collection<Participant> participants) {
    TargetProviderResponse response = new TargetProviderResponse();
    // ask for two containers at a time
    List<ContainerSpec> containersToAcquire = Lists.newArrayList();
    List<Participant> containersToStart = Lists.newArrayList();
    List<Participant> containersToRelease = Lists.newArrayList();
    List<Participant> containersToStop = Lists.newArrayList();

    for (int i = 0; i < DEFAULT_CONTAINER - participants.size(); i++) {
      containersToAcquire.add(new ContainerSpec(ContainerId.from("container"
          + (DEFAULT_CONTAINER - i))));
    }
    response.setContainersToAcquire(containersToAcquire);

    for (Participant participant : participants) {
      ContainerConfig containerConfig = participant.getContainerConfig();
      if (containerConfig != null && containerConfig.getState() != null) {
        ContainerState state = containerConfig.getState();
        switch (state) {
        case ACQUIRED:
          // acquired containers are ready to start
          containersToStart.add(participant);
          break;
        case ACTIVE:

          break;
        case HALTED:
          // halted containers can be released
          // containersToRelease.add(participant);
          break;
        case ACQUIRING:
          break;
        case CONNECTING:
          break;
        case FAILED:
          break;
        case FINALIZED:
          break;
        case FINALIZING:
          break;
        case TEARDOWN:
          break;
        default:
          break;
        }
        ContainerId containerId = containerConfig.getId();
        if (containerId != null) {
          // _containerParticipants.put(containerId, participant.getId());
          // _states.put(containerId, state);
        }
      }
    }
    response.setContainersToStart(containersToStart);
    response.setContainersToRelease(containersToRelease);
    response.setContainersToStop(containersToStop);
    LOG.info("target provider response containers to acquire:" + response.getContainersToAcquire());
    LOG.info("target provider response containers to start:" + response.getContainersToStart());
    return response;
  }

  private ContainerRequest setupContainerAskForRM(ContainerSpec spec) {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    int requestPriority = 0;
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(requestPriority);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    int memory = 1024;
    capability.setMemory(memory);

    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

}
