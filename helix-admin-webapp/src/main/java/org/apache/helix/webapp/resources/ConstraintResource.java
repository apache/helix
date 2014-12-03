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
import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ServerResource;

/**
 * Class for server-side resource at <code>"/clusters/{clusterName}/constraints/{constraintType}"
 * <p>
 * <li>GET list all constraints
 * <li>POST set constraints
 * <li>DELETE remove constraints
 */
public class ConstraintResource extends ServerResource {
  private final static Logger LOG = Logger.getLogger(ConstraintResource.class);

  public ConstraintResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  /**
   * List all constraints
   * <p>
   * Usage: <code>curl http://{host:port}/clusters/{clusterName}/constraints/MESSAGE_CONSTRAINT
   */
  @Override
  public Representation get() {
    StringRepresentation representation = null;
    String clusterName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
    String constraintTypeStr =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CONSTRAINT_TYPE);
    String constraintId =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CONSTRAINT_ID);

    try {
      ConstraintType constraintType = ConstraintType.valueOf(constraintTypeStr);
      ZkClient zkClient =
          ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
      HelixAdmin admin = new ZKHelixAdmin(zkClient);

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
      LOG.error("Exception get constraints", e);
    }

    return representation;
  }

  /**
   * Set constraints
   * <p>
   * Usage:
   * <code>curl -d 'jsonParameters={"constraintAttributes":"RESOURCE={resource},CONSTRAINT_VALUE={1}"}'
   * -H "Content-Type: application/json" http://{host:port}/clusters/{cluster}/constraints/MESSAGE_CONSTRAINT/{constraintId}
   */
  @Override
  public Representation post(Representation entity) {
    String clusterName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
    String constraintTypeStr =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CONSTRAINT_TYPE);
    String constraintId =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CONSTRAINT_ID);

    try {
      ZkClient zkClient =
          ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
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

  /**
   * Remove constraints
   * <p>
   * Usage:
   * <code>curl -X DELETE http://{host:port}/clusters/{cluster}/constraints/MESSAGE_CONSTRAINT/{constraintId}
   */
  @Override
  public Representation delete() {
    String clusterName =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CLUSTER_NAME);
    String constraintTypeStr =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CONSTRAINT_TYPE);
    String constraintId =
        ResourceUtil.getAttributeFromRequest(getRequest(), ResourceUtil.RequestKey.CONSTRAINT_ID);

    try {
      ZkClient zkClient =
          ResourceUtil.getAttributeFromCtx(getContext(), ResourceUtil.ContextKey.ZKCLIENT);
      ClusterSetup setupTool = new ClusterSetup(zkClient);

      setupTool.removeConstraint(clusterName, constraintTypeStr, constraintId);
    } catch (Exception e) {
      LOG.error("Error in delete constraint", e);
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
          MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
    return null;
  }
}
