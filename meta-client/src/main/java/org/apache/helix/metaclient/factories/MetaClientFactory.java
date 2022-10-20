package org.apache.helix.metaclient.factories;

import org.apache.helix.metaclient.api.MetaClientInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory class for MetaClient. It returns MetaClient entity based on config.
 */
public class MetaClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MetaClientFactory.class);

  public MetaClientInterface getMetaClient(MetaClientConfig config) {
    return null;
  }
}
