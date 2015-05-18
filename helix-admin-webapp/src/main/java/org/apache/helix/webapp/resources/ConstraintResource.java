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

import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.webapp.RestAdminApplication;
import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

public class ConstraintResource extends ServerResource {

  private final static Logger LOG = Logger.getLogger(ConstraintResource.class);

  public ConstraintResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  // TODO move to a util function
  String getValue(String key) {
    return (String) getRequest().getAttributes().get(key);
  }

  @Override
  public Representation get() {
    StringRepresentation representation = null;
    String clusterName = getValue("clusterName");
    String constraintTypeStr = getValue("constraintType").toUpperCase();
    String constraintId = getValue("constraintId");

    try {
      ConstraintType constraintType = ConstraintType.valueOf(constraintTypeStr);
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      // ClusterSetup setupTool = new ClusterSetup(zkClient);
      HelixAdmin admin = new ZKHelixAdmin(zkClient); // setupTool.getClusterManagementTool();

      ZNRecord record = admin.getConstraints(clusterName, constraintType).getRecord();
      if (constraintId == null) {
        // get all message constraints
        representation =
            new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(record),
                MediaType.APPLICATION_JSON);
      } else {
        // get a specific constraint
        Map<String, String> constraint = record.getMapField(constraintId);
        if (constraint == null) {
          representation =
              new StringRepresentation("No constraint of type: " + constraintType
                  + " associated with id: " + constraintId, MediaType.APPLICATION_JSON);
        } else {
          ZNRecord subRecord = new ZNRecord(record.getId());
          subRecord.setMapField(constraintId, constraint);
          representation =
              new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(subRecord),
                  MediaType.APPLICATION_JSON);
        }
      }
    } catch (IllegalArgumentException e) {
      representation =
          new StringRepresentation("constraint-type: " + constraintTypeStr + " not recognized.",
              MediaType.APPLICATION_JSON);
    } catch (Exception e) {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      representation = new StringRepresentation(error, MediaType.APPLICATION_JSON);
      LOG.error("", e);
    }

    return representation;
  }

  @Override
  public Representation post(Representation entity) {
    String clusterName = getValue("clusterName");
    String constraintTypeStr = getValue("constraintType").toUpperCase();
    String constraintId = getValue("constraintId");

    try {
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkClient);
      JsonParameters jsonParameters = new JsonParameters(entity);

      String constraintAttrStr = jsonParameters.getParameter(JsonParameters.CONSTRAINT_ATTRIBUTES);
      setupTool.setConstraint(clusterName, constraintTypeStr, constraintId, constraintAttrStr);

    } catch (Exception e) {
      LOG.error("Error in posting " + entity, e);
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    return null;
  }

  @Override
  public Representation delete() {
    String clusterName = getValue("clusterName");
    String constraintTypeStr = getValue("constraintType").toUpperCase();
    String constraintId = getValue("constraintId");

    try {
      ZkClient zkClient =
          (ZkClient) getContext().getAttributes().get(RestAdminApplication.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkClient);

      setupTool.removeConstraint(clusterName, constraintTypeStr, constraintId);

    } catch (Exception e) {
      LOG.error("Error in deleting ", e);
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    return null;
  }
}
