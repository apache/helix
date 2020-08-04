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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.webapp.RestAdminApplication;
import org.apache.zookeeper.data.Stat;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkChildResource extends ServerResource {
  private final static Logger LOG = LoggerFactory.getLogger(ZkChildResource.class);

  public ZkChildResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  private String getZKPath() {
    String relativeRef = getRequest().getResourceRef().getRelativeRef().toString();
    if (relativeRef.equals(".")) {
      relativeRef = "";
    }

    // strip off trailing "/"
    while (relativeRef.endsWith("/")) {
      relativeRef = relativeRef.substring(0, relativeRef.length() - 1);
    }

    return "/" + relativeRef;
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    String zkPath = getZKPath();

    try {
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      ZNRecord result = readZkChild(zkPath, zkClient);

      presentation =
          new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(result),
              MediaType.APPLICATION_JSON);
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      presentation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      LOG.error("Error in read zkPath: " + zkPath, e);
    }

    return presentation;
  }

  private ZNRecord readZkChild(String zkPath, ZkClient zkClient) {
    ZNRecord result = null;

    // read data and stat
    Stat stat = new Stat();
    ZNRecord data = zkClient.readDataAndStat(zkPath, stat, true);
    if (data != null) {
      result = data;
    } else {
      result = new ZNRecord("");
    }

    // read childrenList
    List<String> children = zkClient.getChildren(zkPath);
    if (children != null && children.size() > 0) {
      result.setSimpleField("numChildren", "" + children.size());
      result.setListField("childrenList", children);
    } else {
      result.setSimpleField("numChildren", "" + 0);
    }
    return result;
  }

  @Override
  public Representation delete() {
    String zkPath = getZKPath();
    try {
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);

      List<String> childNames = zkClient.getChildren(zkPath);
      if (childNames != null) {
        for (String childName : childNames) {
          String childPath = zkPath.equals("/") ? "/" + childName : zkPath + "/" + childName;
          zkClient.deleteRecursively(childPath);
        }
      }

      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Error in delete zkChild: " + zkPath, e);
    }
    return null;
  }
}
