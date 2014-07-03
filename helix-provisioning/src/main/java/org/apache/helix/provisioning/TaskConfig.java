package org.apache.helix.provisioning;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class TaskConfig {
  private static final Logger LOG = Logger.getLogger(TaskConfig.class);

  public Map<String, String> config = new HashMap<String, String>();
  public String yamlFile;
  public String name;

  public URI getYamlURI() {
    try {
      return yamlFile != null ? new URI(yamlFile) : null;
    } catch (URISyntaxException e) {
      LOG.error("Error parsing URI for task config", e);
    }
    return null;
  }

  public String getValue(String key) {
    return (config != null ? config.get(key) : null);
  }
}
