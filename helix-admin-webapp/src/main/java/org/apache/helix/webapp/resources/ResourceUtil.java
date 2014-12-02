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

import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.webapp.RestAdminApplication;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.data.Form;
import org.restlet.representation.Representation;

public class ResourceUtil {
  /**
   * Key enums for getting values from request
   */
  public enum RequestKey {
    CLUSTER_NAME("clusterName"),
    JOB_QUEUE("jobQueue"),
    JOB("job");

    private final String _key;

    RequestKey(String key) {
      _key = key;
    }

    public String toString() {
      return _key;
    }
  }

  /**
   * Key enums for getting values from context
   */
  public enum ContextKey {
    ZK_ADDR(RestAdminApplication.ZKSERVERADDRESS),
    ZKCLIENT(RestAdminApplication.ZKCLIENT);

    private final String _key;

    ContextKey(String key) {
      _key = key;
    }

    public String toString() {
      return _key;
    }
  }

  /**
   * Key enums for getting yaml format parameters
   */
  public enum YamlParamKey {
    NEW_JOB("newJob");

    private final String _key;
    YamlParamKey(String key) {
      _key = key;
    }

    public String toString() {
      return _key;
    }
  }

  public static String getAttributeFromRequest(Request r, RequestKey key) {
    return (String) r.getAttributes().get(key.toString());
  }


  public static ZkClient getAttributeFromCtx(Context ctx, ContextKey key) {
    return (ZkClient) ctx.getAttributes().get(key.toString());
  }

  public static String getYamlParameters(Form form, YamlParamKey key) {
    return form.getFirstValue(key.toString());
  }
}
