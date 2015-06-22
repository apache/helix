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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerProvider;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.helix.controller.provisioner.Provisioner;
import org.apache.helix.controller.provisioner.TargetProvider;
import org.apache.helix.controller.provisioner.TargetProviderResponse;
import org.apache.helix.provisioning.ApplicationSpec;
import org.apache.helix.provisioning.ContainerAskResponse;
import org.apache.helix.provisioning.ContainerLaunchResponse;
import org.apache.helix.provisioning.ContainerReleaseResponse;
import org.apache.helix.provisioning.ContainerStopResponse;
import org.apache.helix.provisioning.ParticipantLauncher;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class YarnProvisioner implements Provisioner, TargetProvider, ContainerProvider {

  private static final Log LOG = LogFactory.getLog(YarnProvisioner.class);
  static GenericApplicationMaster applicationMaster;
  static ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors
      .newCachedThreadPool());
  public static AppMasterConfig applicationMasterConfig;
  public static ApplicationSpec applicationSpec;
  Map<ContainerId, Container> allocatedContainersMap = new HashMap<ContainerId, Container>();
  // private HelixManager _helixManager;
  private ResourceConfig _resourceConfig;

  public YarnProvisioner() {

  }

  @Override
  public ListenableFuture<ContainerId> allocateContainer(ContainerSpec spec) {
    ContainerRequest containerAsk = setupContainerAskForRM(spec);
    ListenableFuture<ContainerAskResponse> requestNewContainer =
        applicationMaster.acquireContainer(containerAsk);
    return Futures.transform(requestNewContainer,
        new Function<ContainerAskResponse, ContainerId>() {
          @Override
          public ContainerId apply(ContainerAskResponse containerAskResponse) {
            ContainerId helixContainerId =
                ContainerId.from(containerAskResponse.getContainer().getId().toString());
            allocatedContainersMap.put(helixContainerId, containerAskResponse.getContainer());
            return helixContainerId;
          }
        });

  }

  @Override
  public ListenableFuture<Boolean> deallocateContainer(final ContainerId containerId) {
    ListenableFuture<ContainerReleaseResponse> releaseContainer =
        applicationMaster.releaseContainer(allocatedContainersMap.get(containerId));
    return Futures.transform(releaseContainer, new Function<ContainerReleaseResponse, Boolean>() {
      @Override
      public Boolean apply(ContainerReleaseResponse response) {
        return response != null;
      }
    }, service);

  }

  @Override
  public ListenableFuture<Boolean> startContainer(final ContainerId containerId,
      Participant participant) {
    Container container = allocatedContainersMap.get(containerId);
    ContainerLaunchContext launchContext;
    try {
      launchContext = createLaunchContext(containerId, container, participant);
    } catch (Exception e) {
      LOG.error("Exception while creating context to launch container:" + containerId, e);
      return null;
    }
    ListenableFuture<ContainerLaunchResponse> future =
        applicationMaster.launchContainer(container, launchContext);
    return Futures.transform(future, new Function<ContainerLaunchResponse, Boolean>() {
      @Override
      public Boolean apply(ContainerLaunchResponse response) {
        return response != null;
      }
    }, service);
  }

  private ContainerLaunchContext createLaunchContext(ContainerId containerId, Container container,
      Participant participant) throws Exception {

    ContainerLaunchContext participantContainer = Records.newRecord(ContainerLaunchContext.class);

    // Map<String, String> envs = System.getenv();
    String appName = applicationMasterConfig.getAppName();
    int appId = applicationMasterConfig.getAppId();
    String serviceName = _resourceConfig.getId().stringify();
    String serviceClasspath = applicationMasterConfig.getClassPath(serviceName);
    String mainClass = applicationMasterConfig.getMainClass(serviceName);
    String zkAddress = applicationMasterConfig.getZKAddress();

    // set the localresources needed to launch container
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    LocalResource servicePackageResource = Records.newRecord(LocalResource.class);
    YarnConfiguration conf = new YarnConfiguration();
    FileSystem fs;
    fs = FileSystem.get(conf);
    String pathSuffix = appName + "/" + appId + "/" + serviceName + ".tar";
    Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
    FileStatus destStatus = fs.getFileStatus(dst);

    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need the jar file to be untarred for now
    servicePackageResource.setType(LocalResourceType.ARCHIVE);
    // Set visibility of the resource
    // Setting to most private option
    servicePackageResource.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    servicePackageResource.setResource(ConverterUtils.getYarnUrlFromPath(dst));
    // Set timestamp and length of file so that the framework
    // can do basic sanity checks for the local resource
    // after it has been copied over to ensure it is the same
    // resource the client intended to use with the application
    servicePackageResource.setTimestamp(destStatus.getModificationTime());
    servicePackageResource.setSize(destStatus.getLen());
    LOG.info("Setting local resource:" + servicePackageResource + " for service" + serviceName);
    localResources.put(serviceName, servicePackageResource);

    // Set local resource info into app master container launch context
    participantContainer.setLocalResources(localResources);

    // Set the necessary security tokens as needed
    // amContainer.setContainerTokens(containerToken);

    // Set the env variables to be setup in the env where the application master will be run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();
    env.put(serviceName, dst.getName());
    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv =
        new StringBuilder(Environment.CLASSPATH.$()).append(File.pathSeparatorChar).append("./*");
    classPathEnv.append(File.pathSeparatorChar);
    classPathEnv.append(serviceClasspath);
    for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(File.pathSeparatorChar);
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");
    LOG.info("Setting classpath for service:\n" + classPathEnv.toString());
    env.put("CLASSPATH", classPathEnv.toString());

    participantContainer.setEnvironment(env);

    if (applicationMaster.allTokens != null) {
      LOG.info("Setting tokens: " + applicationMaster.allTokens);
      participantContainer.setTokens(applicationMaster.allTokens);
    }

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + 4096 + "m");
    // Set class name
    vargs.add(ParticipantLauncher.class.getCanonicalName());
    // Set params for container participant
    vargs.add("--zkAddress " + zkAddress);
    vargs.add("--cluster " + appName);
    vargs.add("--participantId " + participant.getId().stringify());
    vargs.add("--participantClass " + mainClass);

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/ContainerParticipant.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/ContainerParticipant.stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up  container launch command " + command.toString()
        + " with arguments \n" + vargs);
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    participantContainer.setCommands(commands);
    return participantContainer;
  }

  @Override
  public ListenableFuture<Boolean> stopContainer(final ContainerId containerId) {
    Container container = allocatedContainersMap.get(containerId);
    ListenableFuture<ContainerStopResponse> future = applicationMaster.stopContainer(container);
    return Futures.transform(future, new Function<ContainerStopResponse, Boolean>() {
      @Override
      public Boolean apply(ContainerStopResponse response) {
        return response != null;
      }
    }, service);
  }

  @Override
  public void init(HelixManager helixManager, ResourceConfig resourceConfig) {
    // _helixManager = helixManager;
    _resourceConfig = resourceConfig;
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
    YarnProvisionerConfig provisionerConfig =
        (YarnProvisionerConfig) cluster.getConfig().getResourceMap().get(resourceId)
            .getProvisionerConfig();
    int targetNumContainers = provisionerConfig.getNumContainers();

    // Any container that is in a state should be put in this set
    Set<ParticipantId> existingContainersIdSet = new HashSet<ParticipantId>();

    // Cache halted containers to determine which to restart and which to release
    Map<ParticipantId, Participant> excessHaltedContainers = Maps.newHashMap();

    // Cache participants to ensure that excess participants are stopped
    Map<ParticipantId, Participant> excessActiveContainers = Maps.newHashMap();

    for (Participant participant : participants) {
      ContainerConfig containerConfig = participant.getContainerConfig();
      if (containerConfig != null && containerConfig.getState() != null) {
        ContainerState state = containerConfig.getState();
        switch (state) {
        case ACQUIRING:
          existingContainersIdSet.add(participant.getId());
          break;
        case ACQUIRED:
          // acquired containers are ready to start
          existingContainersIdSet.add(participant.getId());
          containersToStart.add(participant);
          break;
        case CONNECTING:
          existingContainersIdSet.add(participant.getId());
          break;
        case CONNECTED:
          // active containers can be stopped or kept active
          existingContainersIdSet.add(participant.getId());
          excessActiveContainers.put(participant.getId(), participant);
          break;
        case DISCONNECTED:
          // disconnected containers must be stopped
          existingContainersIdSet.add(participant.getId());
          containersToStop.add(participant);
        case HALTING:
          existingContainersIdSet.add(participant.getId());
          break;
        case HALTED:
          // halted containers can be released or restarted
          existingContainersIdSet.add(participant.getId());
          excessHaltedContainers.put(participant.getId(), participant);
          break;
        case FINALIZING:
          existingContainersIdSet.add(participant.getId());
          break;
        case FINALIZED:
          break;
        case FAILED:
          // remove the failed instance
          // _helixManager.getClusterManagmentTool().dropInstance(cluster.getId().toString(),
          // new InstanceConfig(participant.getId()));
          excessHaltedContainers.put(participant.getId(), participant);
          break;
        default:
          break;
        }
      }
    }

    for (int i = 0; i < targetNumContainers; i++) {
      ParticipantId participantId = ParticipantId.from(resourceId + "_container_" + (i));
      excessActiveContainers.remove(participantId); // don't stop this container if active
      if (excessHaltedContainers.containsKey(participantId)) {
        // Halted containers can be restarted if necessary
        // Participant participant = excessHaltedContainers.get(participantId);
        // containersToStart.add(participant);
        // excessHaltedContainers.remove(participantId); // don't release this container
      } else if (!existingContainersIdSet.contains(participantId)) {
        // Unallocated containers must be allocated
        ContainerSpec containerSpec = new ContainerSpec(participantId);
        int mem = 4096;
        if (_resourceConfig.getUserConfig() != null) {
          mem = _resourceConfig.getUserConfig().getIntField("memory", mem);
        }
        containerSpec.setMemory(mem);
        containersToAcquire.add(containerSpec);
      }
    }

    // Add all the containers that should be stopped because they fall outside the target range
    containersToStop.addAll(excessActiveContainers.values());

    // Add halted containers that should not be restarted
    containersToRelease.addAll(excessHaltedContainers.values());

    response.setContainersToAcquire(containersToAcquire);
    response.setContainersToStart(containersToStart);
    response.setContainersToRelease(containersToRelease);
    response.setContainersToStop(containersToStop);
    LOG.info("target provider response containers to acquire:" + response.getContainersToAcquire());
    LOG.info("target provider response containers to start:" + response.getContainersToStart());
    LOG.info("target provider response containers to stop:" + response.getContainersToStop());
    LOG.info("target provider response containers to release:" + response.getContainersToRelease());
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
    int memory = spec.getMemory();
    capability.setMemory(memory);

    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  @Override
  public ContainerProvider getContainerProvider() {
    return this;
  }

  @Override
  public TargetProvider getTargetProvider() {
    return this;
  }

}
