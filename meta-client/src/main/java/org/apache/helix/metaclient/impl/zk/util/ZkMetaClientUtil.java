package org.apache.helix.metaclient.impl.zk.util;

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

import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.api.OpResult;
import org.apache.helix.metaclient.constants.*;
import org.apache.helix.zookeeper.zkclient.exception.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.server.EphemeralType;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Function;

public class ZkMetaClientUtil {
  //Default ACL value until metaClient Op has ACL of its own.
  private static final List<ACL> DEFAULT_ACL = Collections.unmodifiableList(ZooDefs.Ids.OPEN_ACL_UNSAFE);

  private ZkMetaClientUtil(){
  }

  /**
   * Helper function for transactionOp. Converts MetaClient Op's into Zk Ops to execute
   * zk transactional support.
   * @param ops
   * @return
   */
  public static List<Op> metaClientOpToZk(Iterable<org.apache.helix.metaclient.api.Op> ops) {
    getOpMap().put(org.apache.helix.metaclient.api.Op.Type.CREATE, op -> {
      try {
        CreateMode mode = convertMetaClientMode(((org.apache.helix.metaclient.api.Op.Create) op).getEntryMode());
        return Op.create(op.getPath(), ((org.apache.helix.metaclient.api.Op.Create) op).getData(), DEFAULT_ACL, mode);
      } catch (KeeperException e) {
        throw translateZkExceptionToMetaclientException(ZkException.create(e));
      }
    });

    getOpMap().put(org.apache.helix.metaclient.api.Op.Type.DELETE,
        op -> Op.delete(op.getPath(), ((org.apache.helix.metaclient.api.Op.Delete) op).getVersion()));

    getOpMap().put(org.apache.helix.metaclient.api.Op.Type.SET,
        op -> Op.setData(op.getPath(),
            ((org.apache.helix.metaclient.api.Op.Set) op).getData(), ((org.apache.helix.metaclient.api.Op.Set) op).getVersion()));

    getOpMap().put(org.apache.helix.metaclient.api.Op.Type.CHECK,
        op -> Op.check(op.getPath(), ((org.apache.helix.metaclient.api.Op.Check) op).getVersion()));

    List<Op> zkOps = new ArrayList<>();
    for (org.apache.helix.metaclient.api.Op op : ops) {
      Function<org.apache.helix.metaclient.api.Op, Op> function = getOpMap().get(op.getType());
      if (function != null) {
        zkOps.add(function.apply(op));
      } else {
        throw new IllegalArgumentException("OpResult type " + op.getType().name() + "is not supported.");
      }
    }
    return zkOps;
  }

  private static final class OpmapHolder {
    static final Map<org.apache.helix.metaclient.api.Op.Type, Function<org.apache.helix.metaclient.api.Op, Op>> OPMAP =
        new EnumMap<>(org.apache.helix.metaclient.api.Op.Type.class);
  }

  private static Map<org.apache.helix.metaclient.api.Op.Type, Function<org.apache.helix.metaclient.api.Op, Op>> getOpMap() {
    return OpmapHolder.OPMAP;
  }

  private static CreateMode convertMetaClientMode(MetaClientInterface.EntryMode entryMode) throws KeeperException {
    if (entryMode == MetaClientInterface.EntryMode.PERSISTENT) {
      return CreateMode.fromFlag(0);
    }
    if (entryMode == MetaClientInterface.EntryMode.EPHEMERAL) {
      return CreateMode.fromFlag(1);
    }
    if (entryMode == MetaClientInterface.EntryMode.CONTAINER) {
      return CreateMode.fromFlag(4);
    }
    throw new IllegalArgumentException(entryMode.name() + " is not a supported EntryMode.");
  }

  /**
   * Helper function for transactionOP. Converts the result from calling zk transactional support into
   * metaclient OpResults.
   * @param zkResult
   * @return
   */
  public static List<OpResult> zkOpResultToMetaClient(List<org.apache.zookeeper.OpResult> zkResult) {
    getOpResultMap().put(org.apache.zookeeper.OpResult.CreateResult.class, opResult -> {
      org.apache.zookeeper.OpResult.CreateResult zkOpCreateResult = (org.apache.zookeeper.OpResult.CreateResult) opResult;
      if (opResult.getType() == 1) {
        return new OpResult.CreateResult(zkOpCreateResult.getPath());
      } else {
        MetaClientInterface.Stat metaClientStat = new MetaClientInterface.Stat(convertZkEntryMode(zkOpCreateResult.getStat().getEphemeralOwner()),
            zkOpCreateResult.getStat().getVersion());
        return new OpResult.CreateResult(zkOpCreateResult.getPath(), metaClientStat);
      }});

    getOpResultMap().put(org.apache.zookeeper.OpResult.DeleteResult.class, opResult -> new OpResult.DeleteResult());

    getOpResultMap().put(org.apache.zookeeper.OpResult.GetDataResult.class, opResult -> {
      org.apache.zookeeper.OpResult.GetDataResult zkOpGetDataResult = (org.apache.zookeeper.OpResult.GetDataResult) opResult;
      MetaClientInterface.Stat metaClientStat = new MetaClientInterface.Stat(convertZkEntryMode(zkOpGetDataResult.getStat().getEphemeralOwner()),
          zkOpGetDataResult.getStat().getVersion());
      return new OpResult.GetDataResult(zkOpGetDataResult.getData(), metaClientStat);
    });

    getOpResultMap().put(org.apache.zookeeper.OpResult.SetDataResult.class, opResult -> {
      org.apache.zookeeper.OpResult.SetDataResult zkOpSetDataResult = (org.apache.zookeeper.OpResult.SetDataResult) opResult;
      MetaClientInterface.Stat metaClientStat = new MetaClientInterface.Stat(convertZkEntryMode(zkOpSetDataResult.getStat().getEphemeralOwner()),
          zkOpSetDataResult.getStat().getVersion());
      return new OpResult.SetDataResult(metaClientStat);
    });

    getOpResultMap().put(org.apache.zookeeper.OpResult.GetChildrenResult.class, opResult -> new OpResult.GetChildrenResult(
        ((org.apache.zookeeper.OpResult.GetChildrenResult) opResult).getChildren()));

    getOpResultMap().put(org.apache.zookeeper.OpResult.CheckResult.class, opResult -> new OpResult.CheckResult());

    getOpResultMap().put(org.apache.zookeeper.OpResult.ErrorResult.class, opResult -> new OpResult.ErrorResult(
        ((org.apache.zookeeper.OpResult.ErrorResult) opResult).getErr()));

    List<OpResult> metaClientOpResult = new ArrayList<>();
    for (org.apache.zookeeper.OpResult opResult : zkResult) {
      Function<org.apache.zookeeper.OpResult, OpResult> function = getOpResultMap().get(opResult.getClass());
      if (function != null) {
        metaClientOpResult.add(function.apply(opResult));
      } else {
        throw new IllegalArgumentException("OpResult type " + opResult.getType() + "is not supported.");
      }
    }

    return metaClientOpResult;
  }

  private static final class OpResultMapHolder {
    static final Map<Class<? extends org.apache.zookeeper.OpResult>, Function<org.apache.zookeeper.OpResult, OpResult>> OPRESULTMAP =
        new HashMap<>();
  }

  private static Map<Class<? extends org.apache.zookeeper.OpResult>, Function<org.apache.zookeeper.OpResult, OpResult>> getOpResultMap() {
    return OpResultMapHolder.OPRESULTMAP;
  }

  public static MetaClientInterface.EntryMode convertZkEntryMode(long ephemeralOwner) {
    EphemeralType zkEphemeralType = EphemeralType.get(ephemeralOwner);
    switch (zkEphemeralType) {
      case VOID:
        return MetaClientInterface.EntryMode.PERSISTENT;
      case CONTAINER:
        return MetaClientInterface.EntryMode.CONTAINER;
      case NORMAL:
        return MetaClientInterface.EntryMode.EPHEMERAL;
      // TODO: TTL is not supported now.
      //case TTL:
      //  return EntryMode.TTL;
      default:
        throw new IllegalArgumentException(zkEphemeralType + " is not supported.");
    }
  }

  public static MetaClientException translateZkExceptionToMetaclientException(ZkException e) {
    if (e instanceof ZkNodeExistsException) {
      return new MetaClientNoNodeException(e);
    } else if (e instanceof ZkBadVersionException) {
      return new MetaClientBadVersionException(e);
    } else if (e instanceof ZkTimeoutException) {
      return new MetaClientTimeoutException(e);
    } else if (e instanceof ZkInterruptedException) {
      return new MetaClientInterruptException(e);
    } else {
      return new MetaClientException(e);
    }
  }
}
