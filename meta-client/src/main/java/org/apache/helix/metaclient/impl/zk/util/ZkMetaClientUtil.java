package org.apache.helix.metaclient.impl.zk.util;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
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
    Map<org.apache.helix.metaclient.api.Op.Type, Function<org.apache.helix.metaclient.api.Op, Op>> opMap = new HashMap<>();

    opMap.put(org.apache.helix.metaclient.api.Op.Type.CREATE, op -> {
      try {
        CreateMode mode = convertMetaClientMode(((org.apache.helix.metaclient.api.Op.Create) op).getEntryMode());
        return Op.create(op.getPath(), ((org.apache.helix.metaclient.api.Op.Create) op).getData(), DEFAULT_ACL, mode);
      } catch (KeeperException e) {
        throw translateZkExceptionToMetaclientException(ZkException.create(e));
      }
    });

    opMap.put(org.apache.helix.metaclient.api.Op.Type.DELETE,
        op -> Op.delete(op.getPath(), ((org.apache.helix.metaclient.api.Op.Delete) op).getVersion()));

    opMap.put(org.apache.helix.metaclient.api.Op.Type.SET,
        op -> Op.setData(op.getPath(),
            ((org.apache.helix.metaclient.api.Op.Set) op).getData(), ((org.apache.helix.metaclient.api.Op.Set) op).getVersion()));

    opMap.put(org.apache.helix.metaclient.api.Op.Type.CHECK,
        op -> Op.check(op.getPath(), ((org.apache.helix.metaclient.api.Op.Check) op).getVersion()));

    List<Op> zkOps = new ArrayList<>();
    for (org.apache.helix.metaclient.api.Op op : ops) {
      Op temp = opMap.get(op.getType()).apply(op);
      zkOps.add(temp);
    }
    return zkOps;
  }

  private static CreateMode convertMetaClientMode(MetaClientInterface.EntryMode entryMode) throws KeeperException {
    String mode = entryMode.name();
    if (mode.equals(MetaClientInterface.EntryMode.PERSISTENT.name())) {
      return CreateMode.fromFlag(0);
    }
    if (mode.equals(MetaClientInterface.EntryMode.EPHEMERAL.name())) {
      return CreateMode.fromFlag(1);
    }
    return CreateMode.fromFlag(-1);
  }

  /**
   * Helper function for transactionOP. Converts the result from calling zk transactional support into
   * metaclient OpResults.
   * @param zkResult
   * @return
   */
  public static List<OpResult> zkOpResultToMetaClient(List<org.apache.zookeeper.OpResult> zkResult) {
    Map<Class<? extends org.apache.zookeeper.OpResult>, Function<org.apache.zookeeper.OpResult, OpResult>> map = new HashMap<>();

    map.put(org.apache.zookeeper.OpResult.CreateResult.class, opResult -> {
      org.apache.zookeeper.OpResult.CreateResult zkOpCreateResult = (org.apache.zookeeper.OpResult.CreateResult) opResult;
      if (opResult.getType() == 1) {
        return new OpResult.CreateResult(zkOpCreateResult.getPath());
      } else {
        MetaClientInterface.Stat metaClientStat = new MetaClientInterface.Stat(convertZkEntryMode(zkOpCreateResult.getStat().getEphemeralOwner()),
            zkOpCreateResult.getStat().getVersion());
        return new OpResult.CreateResult(zkOpCreateResult.getPath(), metaClientStat);
      }});

    map.put(org.apache.zookeeper.OpResult.DeleteResult.class, opResult -> new OpResult.DeleteResult());

    map.put(org.apache.zookeeper.OpResult.GetDataResult.class, opResult -> {
      org.apache.zookeeper.OpResult.GetDataResult zkOpGetDataResult = (org.apache.zookeeper.OpResult.GetDataResult) opResult;
      MetaClientInterface.Stat metaClientStat = new MetaClientInterface.Stat(convertZkEntryMode(zkOpGetDataResult.getStat().getEphemeralOwner()),
          zkOpGetDataResult.getStat().getVersion());
      return new OpResult.GetDataResult(zkOpGetDataResult.getData(), metaClientStat);
    });

    map.put(org.apache.zookeeper.OpResult.SetDataResult.class, opResult -> {
      org.apache.zookeeper.OpResult.SetDataResult zkOpSetDataResult = (org.apache.zookeeper.OpResult.SetDataResult) opResult;
      MetaClientInterface.Stat metaClientStat = new MetaClientInterface.Stat(convertZkEntryMode(zkOpSetDataResult.getStat().getEphemeralOwner()),
          zkOpSetDataResult.getStat().getVersion());
      return new OpResult.SetDataResult(metaClientStat);
    });

    map.put(org.apache.zookeeper.OpResult.GetChildrenResult.class, opResult -> new OpResult.GetChildrenResult(
        ((org.apache.zookeeper.OpResult.GetChildrenResult) opResult).getChildren()));

    map.put(org.apache.zookeeper.OpResult.CheckResult.class, opResult -> new OpResult.CheckResult());

    map.put(org.apache.zookeeper.OpResult.ErrorResult.class, opResult -> new OpResult.ErrorResult(
        ((org.apache.zookeeper.OpResult.ErrorResult) opResult).getErr()));

    List<OpResult> metaClientOpResult = new ArrayList<>();
    for (org.apache.zookeeper.OpResult opResult : zkResult) {
      metaClientOpResult.add(map.get(opResult.getClass()).apply(opResult));
    }

    return metaClientOpResult;
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
