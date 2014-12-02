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
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskUtil;
import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

public class JobResource extends ServerResource {
  private final static Logger LOG = Logger.getLogger(JobResource.class);

  public JobResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

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

    // Get job queue context
    JobContext ctx = null;
    String path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);
    HelixPropertyStore<ZNRecord> propertyStore =
        new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(zkClient), path, null);

    ctx = TaskUtil.getJobContext(propertyStore, namespacedJobName);

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
