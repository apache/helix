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
    data = new HelloworldAppSpec();
    data._appConfig = new AppConfig();
    data._appConfig.setValue("k1", "v1");
    data._appName = "testApp";
    data._appMasterPackageUri =
        new File("/Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/target/helix-provisioning-0.7.1-incubating-SNAPSHOT-pkg.tar").toURI().toString();
    data._serviceConfigMap = new HashMap<String, Map<String, String>>();
    data._serviceConfigMap.put("HelloWorld", new HashMap<String, String>());
    data._serviceConfigMap.get("HelloWorld").put("k1", "v1");
    data._serviceMainClassMap = new HashMap<String, String>();
    data._serviceMainClassMap.put("HelloWorld", HelloWorldService.class.getCanonicalName());
    data._servicePackageURIMap = new HashMap<String, String>();
    data._servicePackageURIMap
        .put(
            "HelloWorld",
            new File("/Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/target/helix-provisioning-0.7.1-incubating-SNAPSHOT-pkg.tar").toURI().toString());
    data._services = Arrays.asList(new String[] {
      "HelloWorld"
    });

  }

  @Override
  public ApplicationSpec fromYaml(InputStream yamlFile) {
    return data;
  }

  public static void main(String[] args) {
    DumperOptions options = new DumperOptions();
    options.setPrettyFlow(true);

    Yaml yaml = new Yaml(options);
    HelloworldAppSpec data = new HelloworldAppSpec();
    data._appConfig = new AppConfig();
    data._appConfig.setValue("k1", "v1");
    data._appName = "testApp";
    data._appMasterPackageUri =
        "/Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/helix-provisioning-0.7.1-incubating-SNAPSHOT-pkg.tar";
    data._serviceConfigMap = new HashMap<String, Map<String, String>>();
    data._serviceConfigMap.put("HelloWorld", new HashMap<String, String>());
    data._serviceConfigMap.get("HelloWorld").put("k1", "v1");
    data._serviceMainClassMap = new HashMap<String, String>();
    data._serviceMainClassMap.put("HelloWorld", HelloWorldService.class.getCanonicalName());
    data._servicePackageURIMap = new HashMap<String, String>();
    data._servicePackageURIMap
        .put(
            "HelloWorld",
            "/Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/helix-provisioning-0.7.1-incubating-SNAPSHOT-pkg.tar");
    data._services = Arrays.asList(new String[] {
      "HelloWorld"
    });
    String dump = yaml.dump(data);
    System.out.println(dump);
  }

}
