package org.apache.helix.metaclient.constants;

public interface MetaClientConstants {

  // Stop retrying when we reach timeout
  int DEFAULT_OPERATION_RETRY_TIMEOUT = Integer.MAX_VALUE;

  // maxMsToWaitUntilConnected
  int DEFAULT_CONNECTION_INIT_TIMEOUT = 60 * 1000;

  // When a client becomes partitioned from the metadata service for more than session timeout,
  // new session will be established.
  int DEFAULT_SESSION_TIMEOUT = 30 * 1000;


}
