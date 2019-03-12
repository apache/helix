package org.apache.helix.model;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

public class RESTConfig extends HelixProperty {
  public enum RESTConfigProperty {
    CUSTOMIZED_HEALTH_URL // User customized URL for getting participant health status or partition
                          // health status.
  }

  /**
   * Instantiate REST config for a specific cluster
   * @param cluster the cluster identifier
   */
  public RESTConfig(String cluster) {
    super(cluster);
  }

  /**
   * Instantiate REST config with a pre-populated record
   *
   * @param record a ZNRecord corresponding to a cluster configuration
   */
  public RESTConfig(ZNRecord record) {
    super(record);
  }

  /**
   * Set up the user defined URL for check per participant health / per partition health by combine
   * URL and final endpoint. It must ended without "/"
   *
   * eg: http://*:12345/customized/path/check
   *
   * @param customizedHealthURL
   */
  public void setCustomizedHealthURL(String customizedHealthURL) {
    _record.setSimpleField(RESTConfigProperty.CUSTOMIZED_HEALTH_URL.name(), customizedHealthURL);
  }

  /**
   * Get user defined URL to construct per participant health / partition health
   * Return null if it does not exist.
   *
   * @return
   */
  public String getCustomizedHealthURL() {
    return _record.getSimpleField(RESTConfigProperty.CUSTOMIZED_HEALTH_URL.name());
  }

}
