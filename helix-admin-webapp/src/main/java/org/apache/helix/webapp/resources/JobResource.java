package org.apache.helix.webapp.resources;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskUtil;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for server-side resource at <code>"/clusters/{clusterName}/jobQueues/{jobQueue}/{job}"
 * <p>
 * <li>GET list job info
 */
public class JobResource extends ServerResource {
  private final static Logger LOG = LoggerFactory.getLogger(JobResource.class);

  public JobResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  /**
   * List job info
   * <p>
   * Usage: <code>curl http://{host:port}/clusters/{clusterName}/jobQueues/{jobQueue}/{job}
   */
  @Override
  public Representation get() {
    StringRepresentation presentation;
    String clusterName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
    String jobQueueName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.JOB_QUEUE);
    String jobName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.JOB);

    try {
      presentation = getHostedEntitiesRepresentation(clusterName, jobQueueName, jobName);
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("Fail to get job: " + jobName, e);
    }
    return presentation;
  }


  @Override
  public Representation delete() {
    StringRepresentation representation = null;

    String clusterName = ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
    String jobQueueName = ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.JOB_QUEUE);
    String jobName = ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.JOB);

    ZkClient zkClient =
        ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);

    TaskDriver driver = new TaskDriver(zkClient, clusterName);

    try {
      driver.deleteJob(jobQueueName, jobName);
      getResponse().setStatus(Status.SUCCESS_NO_CONTENT);
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      representation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("Fail to delete job: " + jobName, e);
    }

    return representation;
  }

  StringRepresentation getHostedEntitiesRepresentation(String clusterName, String jobQueueName,
      String jobName) throws Exception {

    ZkClient zkClient =
        ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
    HelixDataAccessor accessor =
        ClusterRepresentationUtil.getClusterDataAccessor(zkClient, clusterName);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // Get job queue config
    String namespacedJobName = TaskUtil.getNamespacedJobName(jobQueueName, jobName);
    HelixProperty jobConfig = accessor.getProperty(keyBuilder.resourceConfig(namespacedJobName));

    TaskDriver taskDriver = new TaskDriver(zkClient, clusterName);

    // Get job queue context
    JobContext ctx = taskDriver.getJobContext(namespacedJobName);

    // Create the result
    ZNRecord hostedEntitiesRecord = new ZNRecord(namespacedJobName);
    if (jobConfig != null) {
      hostedEntitiesRecord.merge(jobConfig.getRecord());
    }
    if (ctx != null) {
      hostedEntitiesRecord.merge(ctx.getRecord());
    }

    StringRepresentation representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(hostedEntitiesRecord),
            MediaType.APPLICATION_JSON);

    return representation;
  }
}
