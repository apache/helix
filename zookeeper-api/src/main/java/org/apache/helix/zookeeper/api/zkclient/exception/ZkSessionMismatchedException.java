package org.apache.helix.zookeeper.api.zkclient.exception;

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
