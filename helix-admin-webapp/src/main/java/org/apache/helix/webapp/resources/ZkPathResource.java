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

import java.util.Date;
import java.util.List;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.webapp.RestAdminApplication;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

public class ZkPathResource extends ServerResource {
  private final static Logger LOG = Logger.getLogger(ZkPathResource.class);

  public ZkPathResource() {
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
  public Representation post(Representation entity) {
    String zkPath = getZKPath();

    try {
      JsonParameters jsonParameters = new JsonParameters(entity);
      String command = jsonParameters.getCommand();

      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);

      if (command.equalsIgnoreCase(JsonParameters.ZK_DELETE_CHILDREN)) {
        List<String> childNames = zkClient.getChildren(zkPath);
        if (childNames != null) {
          for (String childName : childNames) {
            String childPath = zkPath.equals("/") ? "/" + childName : zkPath + "/" + childName;
            zkClient.deleteRecursive(childPath);
          }
        }
      } else {
        throw new HelixException("Unsupported command: " + command + ". Should be one of ["
            + JsonParameters.ZK_DELETE_CHILDREN + "]");
      }

      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Error in post zkPath: " + zkPath, e);
    }
    return null;
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    String zkPath = getZKPath();

    try {
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      ZNRecord result = readZkDataStatAndChild(zkPath, zkClient);

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

  private ZNRecord readZkDataStatAndChild(String zkPath, ZkClient zkClient) {
    ZNRecord result = null;

    // read data and stat
    Stat stat = new Stat();
    ZNRecord data = zkClient.readDataAndStat(zkPath, stat, true);
    if (data != null) {
      result = data;
    } else {
      result = new ZNRecord("");
    }
    result.setSimpleField("zkPath", zkPath);
    result.setSimpleField("stat", stat.toString());
    result.setSimpleField("numChildren", "" + stat.getNumChildren());
    result.setSimpleField("ctime", "" + new Date(stat.getCtime()));
    result.setSimpleField("mtime", "" + new Date(stat.getMtime()));
    result.setSimpleField("dataLength", "" + stat.getDataLength());

    // read childrenList
    List<String> children = zkClient.getChildren(zkPath);
    if (children != null && children.size() > 0) {
      result.setListField("children", children);
    }
    return result;
  }

  @Override
  public Representation delete() {
    String zkPath = getZKPath();
    try {
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      zkClient.deleteRecursive(zkPath);

      getResponse().setStatus(Status.SUCCESS_OK);
    } catch (Exception e) {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
      LOG.error("Error in delete zkPath: " + zkPath, e);
    }
    return null;
  }

}
