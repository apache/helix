package org.apache.helix.model;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;


/**
 * The configuration entry for persisting the client side rest endpoint
 * The rest endpoint is used for helix to fetch the health status or other important status of the participant at runtime
 */
public class RESTConfig extends HelixProperty {
  /**
   * Corresponds to "simpleFields" concept in ZnNode
   */
  public enum SimpleFields {
    /**
     * Customized URL for getting participant(instance)'s health status or partition's health status.
     */
    CUSTOMIZED_HEALTH_URL
  }

  /**
   * Instantiate REST config with a pre-populated record
   *
   * @param record a ZNRecord corresponding to a cluster configuration
   */
  public RESTConfig(ZNRecord record) {
    super(record);
  }

  public RESTConfig(String id) {
    super(id);
  }

  public void set(SimpleFields property, String value) {
    _record.setSimpleField(property.name(), value);
  }

  public String get(SimpleFields property) {
    return _record.getSimpleField(property.name());
  }
}
