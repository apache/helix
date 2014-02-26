package org.apache.helix.provisioning;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.api.Scope;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ResourceId;

public class ServiceConfig extends UserConfig{
	
	public ServiceConfig(Scope<ResourceId> scope) {
	  super(scope);
  }
 
}
