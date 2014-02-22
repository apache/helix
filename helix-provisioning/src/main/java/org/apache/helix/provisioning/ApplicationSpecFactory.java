package org.apache.helix.provisioning;

import java.io.InputStream;

public interface ApplicationSpecFactory {
  
  ApplicationSpec fromYaml(InputStream yamlFile);

}
