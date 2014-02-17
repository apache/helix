package org.apache.helix.provisioning.yarn.example;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.provisioning.yarn.AppConfig;
import org.apache.helix.provisioning.yarn.ApplicationSpec;
import org.apache.helix.provisioning.yarn.ApplicationSpecFactory;
import org.yaml.snakeyaml.Yaml;

public class HelloWordAppSpecFactory implements ApplicationSpecFactory{

  @Override
  public ApplicationSpec fromYaml(InputStream yamlFile) {
    return null;
  }
  
  public static void main(String[] args) {
    Yaml yaml = new Yaml();
    HelloworldAppSpec data = new HelloworldAppSpec();
    data._appConfig = new AppConfig();
    data._appName="testApp";
    data._serviceConfigMap = new HashMap<String, Map<String,String>>();
    data._serviceConfigMap.put("HelloWorld", new HashMap<String, String>());
    data._serviceConfigMap.get("HelloWorld").put("k1", "v1");
    
    String dump = yaml.dump(data);
    System.out.println(dump);
  }

}
