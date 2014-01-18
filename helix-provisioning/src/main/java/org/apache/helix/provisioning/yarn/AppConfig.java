package org.apache.helix.provisioning.yarn;

import java.util.HashMap;
import java.util.Map;


public class AppConfig {
	public Map<String, String> config = new HashMap<String, String>();
	
	public String getValue(String key) {
		return (config != null ? config.get(key) : null);
	}
}
