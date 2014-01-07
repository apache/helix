package org.apache.helix.provisioning.yarn;

import org.apache.hadoop.yarn.api.records.Container;

public class ContainerAskResponse {
  
  Container container;

  public Container getContainer() {
    return container;
  }

  public void setContainer(Container container) {
    this.container = container;
  }
  
}
