package org.apache.helix.provisioning.yarn;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.api.Scope;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ResourceId;

public class ServiceConfig extends UserConfig{
	public Map<String, String> config = new HashMap<String, String>();
	
	public ServiceConfig(Scope<ResourceId> scope) {
	  super(scope);
  }
 
}
