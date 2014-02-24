package org.apache.helix.provisioning.yarn.example;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.provisioning.AppConfig;
import org.apache.helix.provisioning.ApplicationSpec;
import org.apache.helix.provisioning.ApplicationSpecFactory;
import org.apache.helix.provisioning.yarn.example.HelloWorldService;
import org.apache.helix.provisioning.yarn.example.HelloworldAppSpec;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

public class HelloWordAppSpecFactory implements ApplicationSpecFactory {

  @Override
  public ApplicationSpec fromYaml(InputStream inputstream) {
    return (ApplicationSpec) new Yaml().load(inputstream);
    // return data;
  }

  public static void main(String[] args) {

    Yaml yaml = new Yaml();
    InputStream resourceAsStream =
        ClassLoader.getSystemClassLoader().getResourceAsStream("hello_world_app_spec.yaml");
    HelloworldAppSpec spec = yaml.loadAs(resourceAsStream, HelloworldAppSpec.class);
    String dump = yaml.dump(spec);
    System.out.println(dump);

  }
}
