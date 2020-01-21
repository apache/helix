package org.apache.helix.manager.zk;

import org.apache.helix.zookeeper.api.zkclient.exception.ZkException;


/**
 * Exception thrown when an action is taken by an expected zk session which
 * does not match the actual zk session.
 */
public class ZkSessionMismatchedException extends ZkException {

    private static final long serialVersionUID = 1L;

    public ZkSessionMismatchedException(String message) {
        super(message);
    }
}
