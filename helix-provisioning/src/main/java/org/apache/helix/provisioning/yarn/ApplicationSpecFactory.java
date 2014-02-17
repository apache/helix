package org.apache.helix.provisioning.yarn;

import java.io.InputStream;

public interface ApplicationSpecFactory {
  
  ApplicationSpec fromYaml(InputStream yamlFile);

}
