package org.apache.helix.rest.server.resources.helix;

/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.rest.common.ContextPropertyKeys;
import org.apache.helix.rest.server.ServerContext;
import org.apache.helix.rest.server.resources.AbstractResource;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * This class provides methods to access Helix specific objects
 * such as cluster, instance, job, resource, workflow, etc in
 * metadata store.
 */
public class AbstractHelixResource extends AbstractResource {

  public RealmAwareZkClient getRealmAwareZkClient() {
    ServerContext serverContext = getServerContext();
    return serverContext.getRealmAwareZkClient();
  }

  @Deprecated
  public ZkClient getZkClient() {
    return (ZkClient) getRealmAwareZkClient();
  }

  public HelixAdmin getHelixAdmin() {
    ServerContext serverContext = getServerContext();
    return serverContext.getHelixAdmin();
  }

  public ClusterSetup getClusterSetup() {
    ServerContext serverContext = getServerContext();
    return serverContext.getClusterSetup();
  }

  public TaskDriver getTaskDriver(String clusterName) {
    ServerContext serverContext = getServerContext();
    return serverContext.getTaskDriver(clusterName);
  }

  public ConfigAccessor getConfigAccessor() {
    ServerContext serverContext = getServerContext();
    return serverContext.getConfigAccessor();
  }

  public HelixDataAccessor getDataAccssor(String clusterName) {
    ServerContext serverContext = getServerContext();
    return serverContext.getDataAccessor(clusterName);
  }

  protected BaseDataAccessor<byte[]> getByteArrayDataAccessor() {
    return getServerContext().getByteArrayZkBaseDataAccessor();
  }

  protected ZkBucketDataAccessor getZkBucketDataAccessor() {
    return getServerContext().getZkBucketDataAccessor();
  }

  protected static ZNRecord toZNRecord(String data)
      throws IOException {
    return ZNRECORD_READER.readValue(data);
  }

  private ServerContext getServerContext() {
    return (ServerContext) _application.getProperties()
        .get(ContextPropertyKeys.SERVER_CONTEXT.name());
  }
}
