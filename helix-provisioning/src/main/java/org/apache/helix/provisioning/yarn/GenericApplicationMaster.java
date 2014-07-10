/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.helix.provisioning.yarn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.helix.provisioning.ContainerAskResponse;
import org.apache.helix.provisioning.ContainerLaunchResponse;
import org.apache.helix.provisioning.ContainerReleaseResponse;
import org.apache.helix.provisioning.ContainerStopResponse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class GenericApplicationMaster {

  static final Log LOG = LogFactory.getLog(GenericApplicationMaster.class);

  // Configuration
  private Configuration conf;

  // Handle to communicate with the Resource Manager
  AMRMClientAsync<ContainerRequest> amRMClient;

  // Handle to communicate with the Node Manager
  NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  NMCallbackHandler containerListener;

  // Application Attempt Id ( combination of attemptId and fail count )
  private ApplicationAttemptId appAttemptID;

  // TODO
  // For status update for clients - yet to be implemented
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";

  Map<ContainerRequest, SettableFuture<ContainerAskResponse>> containerRequestMap =
      new LinkedHashMap<AMRMClient.ContainerRequest, SettableFuture<ContainerAskResponse>>();
  Map<ContainerId, SettableFuture<ContainerReleaseResponse>> containerReleaseMap =
      new LinkedHashMap<ContainerId, SettableFuture<ContainerReleaseResponse>>();
  Map<ContainerId, SettableFuture<ContainerStopResponse>> containerStopMap =
      new LinkedHashMap<ContainerId, SettableFuture<ContainerStopResponse>>();
  Map<ContainerId, SettableFuture<ContainerLaunchResponse>> containerLaunchResponseMap =
      new LinkedHashMap<ContainerId, SettableFuture<ContainerLaunchResponse>>();
  Set<ContainerId> allocatedContainerSet = Sets.newHashSet();

  ByteBuffer allTokens;

  // Launch threads
  List<Thread> launchThreads = new ArrayList<Thread>();

  public GenericApplicationMaster(ApplicationAttemptId appAttemptID) {
    this.appAttemptID = appAttemptID;
    // Set up the configuration
    conf = new YarnConfiguration();
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging
   */
  private void dumpOutDebugInfo() {

    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
    }

    String cmd = "ls -al";
    Runtime run = Runtime.getRuntime();
    Process pr = null;
    try {
      pr = run.exec(cmd);
      pr.waitFor();

      BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
      String line = "";
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
      buf.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Parse command line options
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   * @throws YarnException
   */
  public boolean start() throws ParseException, IOException, YarnException {

    if (Boolean.getBoolean(System.getenv("debug"))) {
      dumpOutDebugInfo();
    }

    Map<String, String> envs = System.getenv();

    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HOST.name())) {
      throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_PORT.name())) {
      throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
    }

    LOG.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId()
        + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());

    LOG.info("Starting ApplicationMaster");

    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    LOG.info("Credentials: " + credentials);
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info("Processing token: " + token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler(this);
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();

    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response =
        amRMClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort,
            appMasterTrackingUrl);
    // Dump out information about cluster capability as seen by the
    // resource manager
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    return true;
  }

  public ListenableFuture<ContainerAskResponse> acquireContainer(ContainerRequest containerAsk) {
    LOG.info("Requesting container ACQUIRE:" + containerAsk);
    SettableFuture<ContainerAskResponse> future = SettableFuture.create();
    containerRequestMap.put(containerAsk, future);
    amRMClient.addContainerRequest(containerAsk);
    return future;
  }

  public ListenableFuture<ContainerStopResponse> stopContainer(Container container) {
    LOG.info("Requesting container STOP:" + container);
    SettableFuture<ContainerStopResponse> future = SettableFuture.create();
    containerStopMap.put(container.getId(), future);
    nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
    return future;
  }

  public ListenableFuture<ContainerReleaseResponse> releaseContainer(Container container) {
    LOG.info("Requesting container RELEASE:" + container);
    SettableFuture<ContainerReleaseResponse> future = SettableFuture.create();
    synchronized (allocatedContainerSet) {
      if (!allocatedContainerSet.contains(container.getId())) {
        future.set(new ContainerReleaseResponse());
      } else {
        containerReleaseMap.put(container.getId(), future);
        amRMClient.releaseAssignedContainer(container.getId());
      }
    }
    return future;
  }

  public ListenableFuture<ContainerLaunchResponse> launchContainer(Container container,
      ContainerLaunchContext containerLaunchContext) {
    LOG.info("Requesting container LAUNCH:" + container + " :"
        + Joiner.on(" ").join(containerLaunchContext.getCommands()));
    SettableFuture<ContainerLaunchResponse> future = SettableFuture.create();
    containerLaunchResponseMap.put(container.getId(), future);
    nmClientAsync.startContainerAsync(container, containerLaunchContext);
    return future;
  }

  @VisibleForTesting
  NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler(this);
  }

  public void finish() {
    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      } catch (InterruptedException e) {
        LOG.info("Exception thrown in thread join: " + e.getMessage());
        e.printStackTrace();
      }
    }

    // When the application completes, it should stop all running containers
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    appStatus = FinalApplicationStatus.SUCCEEDED;
    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }

    amRMClient.stop();
  }

}
