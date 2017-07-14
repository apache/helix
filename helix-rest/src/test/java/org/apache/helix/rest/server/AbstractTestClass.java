package org.apache.helix.rest.server;

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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.rest.server.auditlog.AuditLogger;
import org.apache.helix.rest.server.filters.AuditLogFilter;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.ZKClientPool;
import org.codehaus.jackson.map.ObjectMapper;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.spi.TestContainer;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class AbstractTestClass extends JerseyTestNg.ContainerPerClassTest {
  protected static final String ZK_ADDR = "localhost:2123";
  protected static final String WORKFLOW_PREFIX = "Workflow_";
  protected static final String JOB_PREFIX = "Job_";
  protected static int NUM_PARTITIONS = 10;
  protected static int NUM_REPLICA = 3;
  protected static ZkServer _zkServer;
  protected static ZkClient _gZkClient;
  protected static ClusterSetup _gSetupTool;
  protected static ConfigAccessor _configAccessor;
  protected static BaseDataAccessor<ZNRecord> _baseAccessor;
  protected static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected static boolean _init = false;

  protected static Set<String> _clusters;
  protected static String _superCluster = "superCluster";
  protected static Map<String, Set<String>> _instancesMap = new HashMap<>();
  protected static Map<String, Set<String>> _liveInstancesMap = new HashMap<>();
  protected static Map<String, Set<String>> _resourcesMap = new HashMap<>();
  protected static Map<String, Map<String, Workflow>> _workflowMap = new HashMap<>();

  protected MockAuditLogger _auditLogger = new MockAuditLogger();

  protected class MockAuditLogger implements AuditLogger {
    List<AuditLog> _auditLogList = new ArrayList<>();

    @Override
    public void write(AuditLog auditLog) {
      _auditLogList.add(auditLog);
    }

    public void clearupLogs() {
      _auditLogList.clear();
    }

    public List<AuditLog> getAuditLogs() {
      return _auditLogList;
    }
  }

  @Override
  protected Application configure() {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.packages(AbstractResource.class.getPackage().getName());
    ServerContext serverContext = new ServerContext(ZK_ADDR);
    resourceConfig.property(ContextPropertyKeys.SERVER_CONTEXT.name(), serverContext);
    resourceConfig.register(new AuditLogFilter(Arrays.<AuditLogger>asList(new MockAuditLogger())));

    return resourceConfig;
  }

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return new TestContainerFactory() {
      @Override
      public TestContainer create(final URI baseUri, DeploymentContext deploymentContext) {
        return new TestContainer() {
          private HelixRestServer _helixRestServer;

          @Override
          public ClientConfig getClientConfig() {
            return null;
          }

          @Override
          public URI getBaseUri() {
            return baseUri;
          }

          @Override
          public void start() {
            try {
              _helixRestServer = new HelixRestServer(ZK_ADDR, baseUri.getPort(), baseUri.getPath(),
                  Arrays.<AuditLogger>asList(_auditLogger));
              _helixRestServer.start();
            } catch (Exception ex) {
              throw new TestContainerException(ex);
            }
          }

          @Override
          public void stop() {
            _helixRestServer.shutdown();
          }
        };
      }
    };
  }

  @BeforeSuite
  public void beforeSuite() throws Exception {
    if (!_init) {
      // TODO: use logging.properties file to config java.util.logging.Logger levels
      java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
      topJavaLogger.setLevel(Level.WARNING);

      // start zk
      _zkServer = TestHelper.startZkServer(ZK_ADDR);
      Assert.assertTrue(_zkServer != null);
      ZKClientPool.reset();

      _gZkClient = new ZkClient(ZK_ADDR, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
          ZkClient.DEFAULT_SESSION_TIMEOUT, new ZNRecordSerializer());
      _gSetupTool = new ClusterSetup(_gZkClient);
      _configAccessor = new ConfigAccessor(_gZkClient);
      _baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);

      // wait for the web service to start
      Thread.sleep(100);

      setup();
      _init = true;
    }
  }

  @AfterSuite
  public void afterSuite() throws Exception {
    ZKClientPool.reset();
    if (_gZkClient != null) {
      _gZkClient.close();
      _gZkClient = null;
    }

    if (_zkServer != null) {
      TestHelper.stopZkServer(_zkServer);
      _zkServer = null;
    }
  }

  protected void setup() throws Exception {
    _clusters = createClusters(3);
    _gSetupTool.addCluster(_superCluster, true);
    _clusters.add(_superCluster);
    for (String cluster : _clusters) {
      Set<String> instances = createInstances(cluster, 10);
      Set<String> liveInstances = startInstances(cluster, instances, 6);
      createResourceConfigs(cluster, 8);
      _workflowMap.put(cluster, createWorkflows(cluster, 3));
      Set<String> resources = createResources(cluster, 8);
      _instancesMap.put(cluster, instances);
      _liveInstancesMap.put(cluster, liveInstances);
      _resourcesMap.put(cluster, resources);
      startController(cluster);
    }
  }

  protected Set<String> createInstances(String cluster, int numInstances) throws Exception {
    Set<String> instances = new HashSet<>();
    for (int i = 0; i < numInstances; i++) {
      String instanceName = cluster + "localhost_" + (12918 + i);
      _gSetupTool.addInstanceToCluster(cluster, instanceName);
      instances.add(instanceName);
    }
    return instances;
  }

  protected Set<String> createResources(String cluster, int numResources) {
    Set<String> resources = new HashSet<>();
    for (int i = 0; i < numResources; i++) {
      String resource = cluster + "_db_" + i;
      _gSetupTool.addResourceToCluster(cluster, resource, NUM_PARTITIONS, "MasterSlave");
      _gSetupTool.rebalanceStorageCluster(cluster, resource, NUM_REPLICA);
      resources.add(resource);
    }
    return resources;
  }

  protected Set<String> createResourceConfigs(String cluster, int numResources) {
    Set<String> resources = new HashSet<>();
    for (int i = 0; i < numResources; i++) {
      String resource = cluster + "_db_" + i;
      org.apache.helix.model.ResourceConfig resourceConfig =
          new org.apache.helix.model.ResourceConfig.Builder(resource).setNumReplica(NUM_REPLICA)
              .build();
      _configAccessor.setResourceConfig(cluster, resource, resourceConfig);
      resources.add(resource);
    }
    return resources;
  }

  protected Set<String> startInstances(String cluster, Set<String> instances,
      int numLiveinstances) {
    Set<String> liveInstances = new HashSet<>();
    int i = 0;
    for (String instance : instances) {
      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, cluster, instance);
      participant.syncStart();
      liveInstances.add(instance);
      if (++i > numLiveinstances) {
        break;
      }
    }
    return liveInstances;
  }

  protected ClusterControllerManager startController(String cluster) {
    String controllerName = "controller-" + cluster;
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, cluster, controllerName);
    controller.syncStart();

    return controller;
  }

  protected Set<String> createClusters(int numClusters) {
    Set<String> clusters = new HashSet<>();
    for (int i = 0; i < numClusters; i++) {
      String cluster = "TestCluster_" + i;
      _gSetupTool.addCluster(cluster, true);
      clusters.add(cluster);
    }

    return clusters;
  }

  protected Map<String, Workflow> createWorkflows(String cluster, int numWorkflows) {
    Map<String, Workflow> workflows = new HashMap<>();
    for (int i = 0; i < numWorkflows; i++) {
      Workflow.Builder workflow = new Workflow.Builder(WORKFLOW_PREFIX + i);
      int j = 0;
      for (JobConfig.Builder job : createJobs(cluster, WORKFLOW_PREFIX + i, 3)) {
        workflow.addJob(JOB_PREFIX + j++, job);
      }
      workflows.put(WORKFLOW_PREFIX + i, workflow.build());
      WorkflowContext workflowContext = TaskTestUtil
          .buildWorkflowContext(WORKFLOW_PREFIX + i, TaskState.IN_PROGRESS,
              System.currentTimeMillis(), TaskState.COMPLETED, TaskState.COMPLETED,
              TaskState.IN_PROGRESS);
      _baseAccessor.set(String.format("/%s/%s%s/%s/%s", cluster, PropertyType.PROPERTYSTORE.name(),
          TaskConstants.REBALANCER_CONTEXT_ROOT, WORKFLOW_PREFIX + i, TaskConstants.CONTEXT_NODE),
          workflowContext.getRecord(), AccessOption.PERSISTENT);
      _configAccessor.setResourceConfig(cluster, WORKFLOW_PREFIX + i, workflow.getWorkflowConfig());
    }
    return workflows;
  }

  protected Set<JobConfig.Builder> createJobs(String cluster, String workflowName, int numJobs) {
    Set<JobConfig.Builder> jobCfgs = new HashSet<>();
    for (int i = 0; i < numJobs; i++) {
      JobConfig.Builder job =
          new JobConfig.Builder().setCommand("DummyCommand").setTargetResource("RESOURCE")
              .setWorkflow(workflowName);
      jobCfgs.add(job);
      JobContext jobContext = TaskTestUtil
          .buildJobContext(System.currentTimeMillis(), System.currentTimeMillis() + 1,
              TaskPartitionState.COMPLETED);
      _baseAccessor.set(String.format("/%s/%s%s/%s/%s", cluster, PropertyType.PROPERTYSTORE.name(),
          TaskConstants.REBALANCER_CONTEXT_ROOT, workflowName + "_" + JOB_PREFIX + i,
          TaskConstants.CONTEXT_NODE), jobContext.getRecord(), AccessOption.PERSISTENT);
      _configAccessor.setResourceConfig(cluster, workflowName + "_" + JOB_PREFIX + i, job.build());
    }
    return jobCfgs;
  }

  protected static ZNRecord toZNRecord(String data) throws IOException {
    return OBJECT_MAPPER.reader(ZNRecord.class).readValue(data);
  }

  protected String get(String uri, int expectedReturnStatus, boolean expectBodyReturned) {
    final Response response = target(uri).request().get();
    Assert.assertEquals(response.getStatus(), expectedReturnStatus);
    Assert.assertEquals(response.getMediaType().getType(), "application");

    String body = response.readEntity(String.class);
    if (expectBodyReturned) {
      Assert.assertNotNull(body);
    }

    return body;
  }

  protected void put(String uri, Map<String, String> queryParams, Entity entity,
      int expectedReturnStatus) {
    WebTarget webTarget = target(uri);
    if (queryParams != null) {
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
    }
    Response response = webTarget.request().put(entity);
    Assert.assertEquals(response.getStatus(), expectedReturnStatus);
  }

  protected void post(String uri, Map<String, String> queryParams, Entity entity,
      int expectedReturnStatus) {
    WebTarget webTarget = target(uri);
    if (queryParams != null) {
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
    }
    Response response = webTarget.request().post(entity);
    Assert.assertEquals(response.getStatus(), expectedReturnStatus);
  }

  protected void delete(String uri, int expectedReturnStatus) {
    final Response response = target(uri).request().delete();
    Assert.assertEquals(response.getStatus(), expectedReturnStatus);
  }

  protected TaskDriver getTaskDriver(String clusterName) {
    return new TaskDriver(_gZkClient, clusterName);
  }
}
