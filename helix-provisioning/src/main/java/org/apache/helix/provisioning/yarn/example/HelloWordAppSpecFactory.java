package org.apache.helix.provisioning.yarn.example;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.provisioning.yarn.AppConfig;
import org.apache.helix.provisioning.yarn.ApplicationSpec;
import org.apache.helix.provisioning.yarn.ApplicationSpecFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

public class HelloWordAppSpecFactory implements ApplicationSpecFactory {

  static HelloworldAppSpec data;

  static {
    HelloworldAppSpec data = new HelloworldAppSpec();
    AppConfig appConfig = new AppConfig();
    appConfig.setValue("k1", "v1");
    data.setAppConfig(appConfig);
    data.setAppName("testApp");
    data.setAppMasterPackageUri(
        "/Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/helix-provisioning-0.7.1-incubating-SNAPSHOT-pkg.tar");
    HashMap<String, Map<String, String>> serviceConfigMap = new HashMap<String, Map<String, String>>();
    serviceConfigMap.put("HelloWorld", new HashMap<String, String>());
    serviceConfigMap.get("HelloWorld").put("k1", "v1");
    data.setServiceConfigMap(serviceConfigMap);
    HashMap<String, String> serviceMainClassMap = new HashMap<String, String>();
    serviceMainClassMap.put("HelloWorld", HelloWorldService.class.getCanonicalName());
    data.setServiceMainClassMap(serviceMainClassMap);
    HashMap<String, String> servicePackageURIMap = new HashMap<String, String>();
    servicePackageURIMap
        .put(
            "HelloWorld",
            "/Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/helix-provisioning-0.7.1-incubating-SNAPSHOT-pkg.tar");
    data.setServicePackageURIMap(servicePackageURIMap);
    data.setServices(Arrays.asList(new String[] {
      "HelloWorld"
    }));  }

  @Override
  public ApplicationSpec fromYaml(InputStream inputstream) {
    return (ApplicationSpec) new Yaml().load(inputstream);
    // return data;
  }

  public static void main(String[] args) {
    DumperOptions options = new DumperOptions();
    options.setPrettyFlow(true);

    Yaml yaml = new Yaml(options);
    HelloworldAppSpec data = new HelloworldAppSpec();
    AppConfig appConfig = new AppConfig();
    appConfig.setValue("k1", "v1");
    data.setAppConfig(appConfig);
    data.setAppName("testApp");
    data.setAppMasterPackageUri(
        "/Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/helix-provisioning-0.7.1-incubating-SNAPSHOT-pkg.tar");
    HashMap<String, Map<String, String>> serviceConfigMap = new HashMap<String, Map<String, String>>();
    serviceConfigMap.put("HelloWorld", new HashMap<String, String>());
    serviceConfigMap.get("HelloWorld").put("k1", "v1");
    data.setServiceConfigMap(serviceConfigMap);
    HashMap<String, String> serviceMainClassMap = new HashMap<String, String>();
    serviceMainClassMap.put("HelloWorld", HelloWorldService.class.getCanonicalName());
    data.setServiceMainClassMap(serviceMainClassMap);
    HashMap<String, String> servicePackageURIMap = new HashMap<String, String>();
    servicePackageURIMap
        .put(
            "HelloWorld",
            "/Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/helix-provisioning-0.7.1-incubating-SNAPSHOT-pkg.tar");
    data.setServicePackageURIMap(servicePackageURIMap);
    data.setServices(Arrays.asList(new String[] {
      "HelloWorld"
    }));
    String dump = yaml.dump(data);
    System.out.println(dump);

    InputStream resourceAsStream = ClassLoader.getSystemClassLoader().getResourceAsStream("hello_world_app_spec.yaml");
    HelloworldAppSpec load = yaml.loadAs(resourceAsStream,HelloworldAppSpec.class);
    String dumpnew = yaml.dump(load);
    System.out.println(dumpnew.equals(dump));
    
    System.out.println("==================================");
    System.out.println(dumpnew);

  }
}
