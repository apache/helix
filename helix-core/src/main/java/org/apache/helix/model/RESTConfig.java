package org.apache.helix.model;

import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


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

  /**
   * Get the base restful endpoint of the instance
   *
   * @param instance The instance
   * @return The base restful endpoint
   */
  public String getBaseUrl(String instance) {
    String baseUrl = get(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL);
    // pre-assumption of the url, must be format of "http://*/path", the wildcard is replaceable by
    // the instance vip
    assert baseUrl.contains("*");
    // pre-assumption of the instance name, must be format of <instanceVip>_<port>
    assert instance.contains("_");
    String instanceVip = instance.substring(0, instance.indexOf('_'));
    return baseUrl.replace("*", instanceVip);
  }
}
