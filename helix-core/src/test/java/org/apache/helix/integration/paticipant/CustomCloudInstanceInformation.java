package org.apache.helix.integration.paticipant;

import org.apache.helix.api.cloud.CloudInstanceInformation;

/**
 * This is a custom implementation of CloudInstanceInformation. It is used to test the functionality
 * of Helix node auto-registration.
 */
public class CustomCloudInstanceInformation implements CloudInstanceInformation {
  private final String _faultDomain;

  public CustomCloudInstanceInformation(String faultDomain) {
    _faultDomain = faultDomain;
  }

  @Override
  public String get(String key) {
    if (key.equals(CloudInstanceField.FAULT_DOMAIN.name())) {
      return _faultDomain;
    }
    return null;
  }
}
