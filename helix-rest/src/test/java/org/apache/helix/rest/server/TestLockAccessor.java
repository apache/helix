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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.annotations.Test;

public class TestLockAccessor extends AbstractTestClass {
  private static final String TEST_CLUSTER = "TestCluster_0";
  private static final String TEST_USER_ID = "testUser";
  private static final String TEST_TIMEOUT = "100000";
  private static final String TEST_URI = "locks/lock";
  private static final String LOCK_TIMEOUT_STR = "lockTimeout";
  private static final String USER_ID_STR = "userId";
  private static final String ZK_ADDR_STR = "zkAddress";
  private static final String LOCK_PATH_STR = "lockPath";
  private static final String CLUSTER_NAME_STR = "cluster";
  private static final String PARTICIPANT_NAME_STR = "participant";

  @Test
  public void testLockAccessorWithHelixLockScope() throws JsonProcessingException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    Map<String, String> validHelixLockScope = new HashMap<String, String>() {{
      put("property", "PARTICIPANT");
      put(CLUSTER_NAME_STR, TEST_CLUSTER);
      put(PARTICIPANT_NAME_STR, "testParticipant");
    }};
    Map<String, Map<String, String>> validMapFields = new HashMap<String, Map<String, String>>() {{
      put("HelixLockScope", validHelixLockScope);
    }};

    // A valid lock scope is provided
    ZNRecord validPayload = new ZNRecord("validPayload");
    validPayload.setSimpleField(LOCK_TIMEOUT_STR, TEST_TIMEOUT);
    validPayload.setSimpleField(USER_ID_STR, TEST_USER_ID);
    validPayload.setSimpleField(ZK_ADDR_STR, ZK_ADDR);
    validPayload.setMapFields(validMapFields);
    Entity validEntity = Entity
        .entity(OBJECT_MAPPER.writeValueAsString(validPayload), MediaType.APPLICATION_JSON_TYPE);
    post(TEST_URI, null, Entity
        .entity(validEntity, MediaType.APPLICATION_JSON_TYPE), Response.Status.OK.getStatusCode());

    // Another valid lock scope, same path as the previous one. Try to lock while the lock is held by someone else.
    validPayload.setSimpleField(USER_ID_STR, "testUser1");
    post(TEST_URI, null, validEntity, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());

    // An invalid lock scope is provided
    ZNRecord invalidLockScopePayload = new ZNRecord("invalidLockScopePayload");
    invalidLockScopePayload.setSimpleField(LOCK_TIMEOUT_STR, TEST_TIMEOUT);
    invalidLockScopePayload.setSimpleField(USER_ID_STR, TEST_USER_ID);
    invalidLockScopePayload.setSimpleField(ZK_ADDR_STR, ZK_ADDR);
    Map<String, String> invalidHelixLockScope = new HashMap<String, String>() {{
      put("property", "PARTICIPANT");
      put(CLUSTER_NAME_STR, TEST_CLUSTER);
    }};
    Map<String, Map<String, String>> invalidMapFields = new HashMap<String, Map<String, String>>() {{
      put("HelixLockScope", invalidHelixLockScope);
    }};
    invalidLockScopePayload.setMapFields(invalidMapFields);
    Entity invalidLockScopeEntity = Entity
        .entity(OBJECT_MAPPER.writeValueAsString(invalidLockScopePayload),
            MediaType.APPLICATION_JSON_TYPE);
    post(TEST_URI, null, invalidLockScopeEntity, Response.Status.BAD_REQUEST.getStatusCode());

    // No ZK address is provided
    ZNRecord noZkAddrPayload = new ZNRecord("noZkAddrPayload");
    noZkAddrPayload.setSimpleField(LOCK_TIMEOUT_STR, TEST_TIMEOUT);
    noZkAddrPayload.setSimpleField(USER_ID_STR, TEST_USER_ID);
    noZkAddrPayload.setMapFields(validMapFields);
    Entity noZkAddrEntity = Entity
        .entity(OBJECT_MAPPER.writeValueAsString(noZkAddrPayload), MediaType.APPLICATION_JSON_TYPE);
    post(TEST_URI, null, noZkAddrEntity, Response.Status.BAD_REQUEST.getStatusCode());

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test(dependsOnMethods = "testLockAccessorWithHelixLockScope")
  public void testLockAccessorWithUnknownScope() throws JsonProcessingException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());

    ZNRecord validPayload = new ZNRecord("validPayload");
    validPayload.setSimpleField(LOCK_TIMEOUT_STR, TEST_TIMEOUT);
    validPayload.setSimpleField(USER_ID_STR, TEST_USER_ID);
    validPayload.setSimpleField(ZK_ADDR_STR, ZK_ADDR);
    validPayload.setSimpleField(LOCK_PATH_STR, String.format("/%s/LOCK/RESOURCE/%s", TEST_CLUSTER, "testResource"));
    Entity validEntity = Entity
        .entity(OBJECT_MAPPER.writeValueAsString(validPayload), MediaType.APPLICATION_JSON_TYPE);
    post(TEST_URI, null, validEntity, Response.Status.OK.getStatusCode());

    System.out.println("End test :" + TestHelper.getTestMethodName());
  }
}
