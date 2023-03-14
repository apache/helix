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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.api.OpResult;
import org.apache.helix.metaclient.exception.MetaClientBadVersionException;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.exception.MetaClientInterruptException;
import org.apache.helix.metaclient.exception.MetaClientNoNodeException;
import org.apache.helix.metaclient.exception.MetaClientTimeoutException;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.server.EphemeralType;

public class ZkMetaClientUtil {
  //TODO Implement MetaClient ACL
  //Default ACL value until metaClient Op has ACL of its own.
  private static final List<ACL> DEFAULT_ACL =
      Collections.unmodifiableList(ZooDefs.Ids.OPEN_ACL_UNSAFE);

  private ZkMetaClientUtil() {
  }

  /**
   * Helper function for transactionOp. Converts MetaClient Op's into Zk Ops to execute
   * zk transactional support.
   * @param ops
   * @return
   */
  public static List<Op> metaClientOpsToZkOps(Iterable<org.apache.helix.metaclient.api.Op> ops) {
    List<Op> zkOps = new ArrayList<>();
    for (org.apache.helix.metaclient.api.Op op : ops) {
      Function<org.apache.helix.metaclient.api.Op, Op> function = getOpMap().get(op.getType());
      if (function != null) {
        zkOps.add(function.apply(op));
      } else {
        throw new IllegalArgumentException("Op type " + op.getType().name() + " is not supported.");
      }
    }
    return zkOps;
  }

  private static final class OpMapHolder {
    static final Map<org.apache.helix.metaclient.api.Op.Type, Function<org.apache.helix.metaclient.api.Op, Op>> OPMAP = initializeOpMap();

    private static Map<org.apache.helix.metaclient.api.Op.Type, Function<org.apache.helix.metaclient.api.Op, Op>> initializeOpMap() {
      Map<org.apache.helix.metaclient.api.Op.Type, Function<org.apache.helix.metaclient.api.Op, Op>> opmap =
          new EnumMap<>(org.apache.helix.metaclient.api.Op.Type.class);

      opmap.put(org.apache.helix.metaclient.api.Op.Type.CREATE, op -> {
        try {
          CreateMode mode = convertMetaClientMode(
              ((org.apache.helix.metaclient.api.Op.Create) op).getEntryMode());
          return Op.create(op.getPath(), ((org.apache.helix.metaclient.api.Op.Create) op).getData(),
              DEFAULT_ACL, mode);
        } catch (KeeperException e) {
          throw translateZkExceptionToMetaclientException(ZkException.create(e));
        }
      });

      opmap.put(org.apache.helix.metaclient.api.Op.Type.DELETE, op -> Op
          .delete(op.getPath(), ((org.apache.helix.metaclient.api.Op.Delete) op).getVersion()));

      opmap.put(org.apache.helix.metaclient.api.Op.Type.SET, op -> Op
          .setData(op.getPath(), ((org.apache.helix.metaclient.api.Op.Set) op).getData(),
              ((org.apache.helix.metaclient.api.Op.Set) op).getVersion()));

      opmap.put(org.apache.helix.metaclient.api.Op.Type.CHECK, op -> Op
          .check(op.getPath(), ((org.apache.helix.metaclient.api.Op.Check) op).getVersion()));

      return opmap;
    }
  }

  private static Map<org.apache.helix.metaclient.api.Op.Type, Function<org.apache.helix.metaclient.api.Op, Op>> getOpMap() {
    return OpMapHolder.OPMAP;
  }

  public static CreateMode convertMetaClientMode(MetaClientInterface.EntryMode entryMode) throws KeeperException {
    switch (entryMode) {
      case PERSISTENT:
        return CreateMode.PERSISTENT;
      case EPHEMERAL:
        return CreateMode.EPHEMERAL;
      case CONTAINER:
        return CreateMode.CONTAINER;
      default:
        throw new IllegalArgumentException(entryMode.name() + " is not a supported EntryMode.");
    }
  }

  /**
   * Helper function for transactionOP. Converts the result from calling zk transactional support into
   * metaclient OpResults.
   * @param zkResult
   * @return
   */
  public static List<OpResult> zkOpResultToMetaClientOpResults(List<org.apache.zookeeper.OpResult> zkResult) {
    List<OpResult> metaClientOpResult = new ArrayList<>();
    for (org.apache.zookeeper.OpResult opResult : zkResult) {
      Function<org.apache.zookeeper.OpResult, OpResult> function =
          getOpResultMap().get(opResult.getClass());
      if (function != null) {
        metaClientOpResult.add(function.apply(opResult));
      } else {
        throw new IllegalArgumentException(
            "OpResult type " + opResult.getType() + "is not supported.");
      }
    }

    return metaClientOpResult;
  }

  private static final class OpResultMapHolder {
    static final Map<Class<? extends org.apache.zookeeper.OpResult>, Function<org.apache.zookeeper.OpResult, OpResult>>
        OPRESULTMAP = initializeOpResultMap();

    private static Map<Class<? extends org.apache.zookeeper.OpResult>, Function<org.apache.zookeeper.OpResult, OpResult>> initializeOpResultMap() {
      Map<Class<? extends org.apache.zookeeper.OpResult>, Function<org.apache.zookeeper.OpResult, OpResult>>
          opResultMap = new HashMap<>();
      opResultMap.put(org.apache.zookeeper.OpResult.CreateResult.class, opResult -> {
        org.apache.zookeeper.OpResult.CreateResult zkOpCreateResult =
            (org.apache.zookeeper.OpResult.CreateResult) opResult;
        if (opResult.getType() == 1) {
          return new OpResult.CreateResult(zkOpCreateResult.getPath());
        } else {
          MetaClientInterface.Stat metaClientStat = new MetaClientInterface.Stat(
              convertZkEntryModeToMetaClientEntryMode(
                  zkOpCreateResult.getStat().getEphemeralOwner()),
              zkOpCreateResult.getStat().getVersion());
          return new OpResult.CreateResult(zkOpCreateResult.getPath(), metaClientStat);
        }
      });

      opResultMap.put(org.apache.zookeeper.OpResult.DeleteResult.class,
          opResult -> new OpResult.DeleteResult());

      opResultMap.put(org.apache.zookeeper.OpResult.GetDataResult.class, opResult -> {
        org.apache.zookeeper.OpResult.GetDataResult zkOpGetDataResult =
            (org.apache.zookeeper.OpResult.GetDataResult) opResult;
        MetaClientInterface.Stat metaClientStat = new MetaClientInterface.Stat(
            convertZkEntryModeToMetaClientEntryMode(
                zkOpGetDataResult.getStat().getEphemeralOwner()),
            zkOpGetDataResult.getStat().getVersion());
        return new OpResult.GetDataResult(zkOpGetDataResult.getData(), metaClientStat);
      });

      opResultMap.put(org.apache.zookeeper.OpResult.SetDataResult.class, opResult -> {
        org.apache.zookeeper.OpResult.SetDataResult zkOpSetDataResult =
            (org.apache.zookeeper.OpResult.SetDataResult) opResult;
        MetaClientInterface.Stat metaClientStat = new MetaClientInterface.Stat(
            convertZkEntryModeToMetaClientEntryMode(
                zkOpSetDataResult.getStat().getEphemeralOwner()),
            zkOpSetDataResult.getStat().getVersion());
        return new OpResult.SetDataResult(metaClientStat);
      });

      opResultMap.put(org.apache.zookeeper.OpResult.GetChildrenResult.class,
          opResult -> new OpResult.GetChildrenResult(
              ((org.apache.zookeeper.OpResult.GetChildrenResult) opResult).getChildren()));

      opResultMap.put(org.apache.zookeeper.OpResult.CheckResult.class,
          opResult -> new OpResult.CheckResult());

      opResultMap.put(org.apache.zookeeper.OpResult.ErrorResult.class,
          opResult -> new OpResult.ErrorResult(
              ((org.apache.zookeeper.OpResult.ErrorResult) opResult).getErr()));

      return opResultMap;
    }
  }

  private static Map<Class<? extends org.apache.zookeeper.OpResult>, Function<org.apache.zookeeper.OpResult, OpResult>> getOpResultMap() {
    return OpResultMapHolder.OPRESULTMAP;
  }

  public static MetaClientInterface.EntryMode convertZkEntryModeToMetaClientEntryMode(
      long ephemeralOwner) {
    EphemeralType zkEphemeralType = EphemeralType.get(ephemeralOwner);
    switch (zkEphemeralType) {
      case VOID:
        return MetaClientInterface.EntryMode.PERSISTENT;
      case CONTAINER:
        return MetaClientInterface.EntryMode.CONTAINER;
      case NORMAL:
        return MetaClientInterface.EntryMode.EPHEMERAL;
      case TTL:
        return MetaClientInterface.EntryMode.TTL;
      default:
        throw new IllegalArgumentException(zkEphemeralType + " is not supported.");
    }
  }

  public static MetaClientException translateZkExceptionToMetaclientException(ZkException e) {
    if (e instanceof ZkNoNodeException) {
      return new MetaClientNoNodeException(e);
    } else if (e instanceof ZkBadVersionException) {
      return new MetaClientBadVersionException(e);
    } else if (e instanceof ZkTimeoutException) {
      return new MetaClientTimeoutException(e);
    } else if (e instanceof ZkInterruptedException) {
      return new MetaClientInterruptException(e);
    }
    return new MetaClientException(e);
  }

  public static MetaClientInterface.ConnectState translateKeeperStateToMetaClientConnectState(
      Watcher.Event.KeeperState keeperState) {
    if (keeperState == null)
      return MetaClientInterface.ConnectState.NOT_CONNECTED;
    switch (keeperState) {
      case AuthFailed:
        return MetaClientInterface.ConnectState.AUTH_FAILED;
      case Closed:
        return MetaClientInterface.ConnectState.CLOSED_BY_CLIENT;
      case Disconnected:
        return MetaClientInterface.ConnectState.DISCONNECTED;
      case Expired:
        return MetaClientInterface.ConnectState.EXPIRED;
      case SaslAuthenticated:
        return MetaClientInterface.ConnectState.AUTHENTICATED;
      case SyncConnected:
      case ConnectedReadOnly:
        return MetaClientInterface.ConnectState.CONNECTED;
      default:
        throw new IllegalArgumentException(keeperState + " is not a supported.");
    }
  }

  /**
   * This function translate and group Zk exception code to metaclient code.
   * It currently includes all ZK code on 3.6.3.
   */
  public static MetaClientException.ReturnCode translateZooKeeperCodeToMetaClientCode(
      KeeperException.Code zkCode) {
    // TODO: add log to track ZK origional code.
    switch (zkCode) {
      case AUTHFAILED:
      case SESSIONCLOSEDREQUIRESASLAUTH:
      case INVALIDACL:
        return MetaClientException.ReturnCode.AUTH_FAILED;

      case CONNECTIONLOSS:
        return MetaClientException.ReturnCode.CONNECTION_LOSS;

      case BADARGUMENTS:
        return MetaClientException.ReturnCode.INVALID_ARGUMENTS;

      case BADVERSION:
        return MetaClientException.ReturnCode.BAD_VERSION;

      case NOAUTH:
        return MetaClientException.ReturnCode.NO_AUTH;

      case NOWATCHER:
        return MetaClientException.ReturnCode.INVALID_LISTENER;

      case NOTEMPTY:
        return MetaClientException.ReturnCode.NOT_LEAF_ENTRY;

      case NODEEXISTS:
        return MetaClientException.ReturnCode.ENTRY_EXISTS;

      case SESSIONEXPIRED:
      case SESSIONMOVED:
      case UNKNOWNSESSION:
        return MetaClientException.ReturnCode.SESSION_ERROR;

      case NONODE:
        return MetaClientException.ReturnCode.NO_SUCH_ENTRY;

      case OPERATIONTIMEOUT:
        return MetaClientException.ReturnCode.OPERATION_TIMEOUT;

      case OK:
        return MetaClientException.ReturnCode.OK;

      case UNIMPLEMENTED:
        return MetaClientException.ReturnCode.UNIMPLEMENTED;

      case RUNTIMEINCONSISTENCY:
      case DATAINCONSISTENCY:
        return MetaClientException.ReturnCode.CONSISTENCY_ERROR;

      case SYSTEMERROR:
      case MARSHALLINGERROR:
      case NEWCONFIGNOQUORUM:
      case RECONFIGINPROGRESS:
        return MetaClientException.ReturnCode.DB_SYSTEM_ERROR;

      case NOCHILDRENFOREPHEMERALS:
      case INVALIDCALLBACK:
      case NOTREADONLY:
      case EPHEMERALONLOCALSESSION:
      case RECONFIGDISABLED:
        return MetaClientException.ReturnCode.DB_USER_ERROR;

      /*
       * APIERROR is ZooKeeper Code value separator. It is never thrown by ZK server,
       * ZK error codes greater than its value are user or client errors and values less than
       * this indicate a ZK server.
       * Note: there are some mismatch between ZK doc and Zk code intValue define. We are comparing
       * ordinal instead of using intValue().
       *   https://zookeeper.apache.org/doc/r3.6.2/apidocs/zookeeper-server/index.html?org/apache/zookeeper/KeeperException.Code.html
       */
      default:
        if (zkCode.ordinal() < KeeperException.Code.APIERROR.ordinal()
            && zkCode.ordinal() >= KeeperException.Code.SYSTEMERROR.ordinal()) {
          return MetaClientException.ReturnCode.DB_SYSTEM_ERROR;
        }
        return MetaClientException.ReturnCode.DB_USER_ERROR;
    }
  }
}
