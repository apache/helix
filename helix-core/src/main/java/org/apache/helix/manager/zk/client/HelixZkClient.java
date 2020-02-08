package org.apache.helix.manager.zk.client;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.helix.zookeeper.api.zkclient.DataUpdater;
import org.apache.helix.zookeeper.api.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.api.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.api.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.api.zkclient.callback.ZkAsyncCallbacks;
import org.apache.helix.zookeeper.api.zkclient.exception.ZkTimeoutException;
import org.apache.helix.zookeeper.api.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.api.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.api.zkclient.serialize.SerializableSerializer;
import org.apache.helix.zookeeper.api.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * NOTE: this interface has been deprecated. Please use HelixZkClient or RealmAwareZkClient in zookeeper-api instead.
 * HelixZkClient interface.
 */
@Deprecated
public interface HelixZkClient extends org.apache.helix.zookeeper.api.HelixZkClient {
}
