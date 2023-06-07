package org.apache.helix.integration.paticipant;

import java.util.Collections;
import java.util.List;

import org.apache.helix.HelixCloudProperty;
import org.apache.helix.api.cloud.CloudInstanceInformation;
import org.apache.helix.api.cloud.CloudInstanceInformationProcessor;
import org.apache.helix.cloud.azure.AzureCloudInstanceInformation;

/**
 * This is a custom implementation of CloudInstanceInformationProcessor.
 * It is used to test the functionality of Helix node auto-registration.
 */
public class CustomCloudInstanceInformationProcessor implements CloudInstanceInformationProcessor<String> {
  public CustomCloudInstanceInformationProcessor(HelixCloudProperty helixCloudProperty) {
  }

  @Override
  public List<String> fetchCloudInstanceInformation() {
    return Collections.singletonList("response");
  }

  @Override
  public CloudInstanceInformation parseCloudInstanceInformation(List<String> responses) {
    return new AzureCloudInstanceInformation.Builder().setFaultDomain("rack=A:123, host=").build();
  }
}