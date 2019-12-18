package org.apache.helix.manager.zk;

import org.I0Itec.zkclient.exception.ZkException;
import org.apache.zookeeper.KeeperException;


/**
 * Exception thrown when an action is taken by an expected zk session which
 * does not match the actual zk session.
 */
public class ZkSessionMismatchedException extends ZkException {

    private static final long serialVersionUID = 1L;

    public ZkSessionMismatchedException() {
        super();
    }

    public ZkSessionMismatchedException(KeeperException cause) {
        super(cause);
    }

    public ZkSessionMismatchedException(String message, KeeperException cause) {
        super(message, cause);
    }

    public ZkSessionMismatchedException(String message) {
        super(message);
    }
}
