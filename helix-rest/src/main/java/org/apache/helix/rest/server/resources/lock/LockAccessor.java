package org.apache.helix.rest.server.resources.lock;

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
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Lists;
import org.apache.helix.HelixException;
import org.apache.helix.lock.LockInfo;
import org.apache.helix.lock.LockScope;
import org.apache.helix.lock.helix.HelixLockScope;
import org.apache.helix.lock.helix.ZKDistributedNonblockingLock;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.resources.helix.AbstractHelixResource;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/locks")
public class LockAccessor extends AbstractHelixResource {
  private static final Logger LOG = LoggerFactory.getLogger(LockAccessor.class);
  private static final String ZK_ADDR = "zkAddress";
  private static final String LOCK_PATH = "lockPath";
  private static final String LOCK_TIMEOUT = "lockTimeout";
  private static final String USER_ID = "userId";
  private static final String LOCK_MSG = "lockMsg";
  private static final String LOCK_SCOPE_PROPERTY = "property";
  private static final String PATH_KEYS = "pathKeys";
  private static final String PATH_KEY_DELIMITER = "\\|";

  private enum LockScopeKey {
    HelixLockScope,
  }

  /**
   *  A class contains information to locate a lock znode
   */
  private class LockLocator {
    private final String _zkAddress;
    private final String _lockPath;

    private LockLocator(String zkAddress, String lockPath) {
      this._zkAddress = zkAddress;
      this._lockPath = lockPath;
    }

    public String getZkAddress() {
      return _zkAddress;
    }

    public String getLockPath() {
      return _lockPath;
    }
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("lock")
  public Response tryLock(String content) {
    System.out.println("Payload:\n" + content);
    ZKDistributedNonblockingLock lock;
    LockLocator lockLocator;
    LockInfo lockInfo;
    try {
      // Parse payload data
      ZNRecord lockContent = toZNRecord(content);

      // Parse zk address and lock path
      lockLocator = parseLockLocator(lockContent);

      // Parse lock info such as user id, message, timeout
      lockInfo = parseLockInfo(lockContent);

      // Build a lock object
      lock = new ZKDistributedNonblockingLock.Builder().setLockMsg(lockInfo.getMessage())
          .setUserId(lockInfo.getOwner()).setTimeout(lockInfo.getTimeout())
          .setZkAddress(lockLocator.getZkAddress()).setLockPath(lockLocator.getLockPath()).build();
    } catch (Exception e) {
      return badRequest(e.getMessage());
    }

    // Perform try lock
    boolean result;
    try {
      result = lock.tryLock();
    } catch (HelixException e) {
      return badRequest(
          "Lock path is not found or lock scope could not be generated, please examine the request.");
    }

    return result ? OK(JSONRepresentation(lock)) : serverError(String.format(
        "Failed to lock the path %s using zk address %s, please check if this lock is held by someone else.",
        lockLocator.getLockPath(), lockLocator.getZkAddress()));
  }

  private LockLocator parseLockLocator(ZNRecord znRecord) throws Exception {
    if (znRecord == null) {
      return null;
    }

    // Parse zk address from simple field
    String zkAddr = znRecord.getSimpleField(ZK_ADDR);
    if (zkAddr == null) {
      throw new Exception("No ZK address is provided, could not create a lock.");
    }

    // Parse lock path from simple field
    String lockPath = znRecord.getSimpleField(LOCK_PATH);

    // If lock path is not provided, generate a lock path from lock scope
    if (lockPath == null) {
      Map<String, Map<String, String>> mapFields = znRecord.getMapFields();
      LockScope lockScope = null;
      for (LockScopeKey lockScopeKey : LockScopeKey.values()) {
        // If a known lock scope is found, currently only HelixLockScope is available
        if (mapFields.containsKey(lockScopeKey.name())) {
          // Helix Lock Scope
          if (lockScopeKey.name().equals(LockScopeKey.HelixLockScope.name())) {
            lockScope = generateHelixLockScope(mapFields.get(lockScopeKey.name()));
          }
        }
      }
      if (lockScope == null) {
        throw new Exception(
            "Failed to parse a lock scope. Please check the lock scope property and path keys and make sure they are correct.");
      }
      return new LockLocator(zkAddr, lockScope.getPath());
    }

    return new LockLocator(zkAddr, lockPath);
  }

  private LockInfo parseLockInfo(ZNRecord znRecord) throws Exception {
    if (znRecord == null) {
      return null;
    }
    // Parse user iod
    String userId = znRecord.getSimpleField(USER_ID);

    // Parse lock timeout
    long lockTimeout;
    try {
      lockTimeout = Long.parseLong(znRecord.getSimpleField(LOCK_TIMEOUT));
    } catch (NumberFormatException e) {
      throw new Exception("Please check the timeout value input, could not convert to long.");
    }

    // Parse lock message
    String lockMsg = znRecord.getSimpleField(LOCK_MSG);
    return new LockInfo(userId, lockMsg, lockTimeout);
  }

  private LockScope generateHelixLockScope(Map<String, String> lockScopeVal) throws Exception {
    // Get the lock scope property: cluster, participant, resource, etc.
    String CLUSTER = "cluster";
    String RESOURCE = "resource";
    String PARTICIPANT = "participant";
    String lockScopeProperty = lockScopeVal.get(LOCK_SCOPE_PROPERTY);
    HelixLockScope.LockScopeProperty property;
    try {
      property = HelixLockScope.LockScopeProperty.valueOf(lockScopeProperty);
    } catch (IllegalArgumentException e) {
      throw new Exception(String.format(
          "User requested lock scope property %s, but this property does not exist in HelixLockScope.",
          lockScopeProperty));
    }

    // Parse the path keys to construct the lock path
    List<String> pathKeys = Lists.newArrayList();
    switch (property) {
      case CLUSTER: {
        String clusterName = lockScopeVal.get(CLUSTER);
        if (clusterName != null) {
          pathKeys.add(clusterName);
        }
      }
      break;
      case RESOURCE: {
        String clusterName = lockScopeVal.get(CLUSTER);
        String resourceName = lockScopeVal.get(RESOURCE);
        if (clusterName != null && resourceName != null) {
          pathKeys.add(clusterName);
          pathKeys.add(resourceName);
        }
      }
      break;
      case PARTICIPANT: {
        String clusterName = lockScopeVal.get(CLUSTER);
        String participantName = lockScopeVal.get(PARTICIPANT);
        if (clusterName != null && participantName != null) {
          pathKeys.add(clusterName);
          pathKeys.add(participantName);
        }
      }
      default:
        // Not gonna happen
    }
    if (pathKeys.isEmpty()) {
      throw new Exception(
          "Not enough path keys are provided to construct the lock path. Please check the request.");
    }
    return new HelixLockScope(property, pathKeys);
  }
}
