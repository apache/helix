package org.apache.helix.provisioning.yarn.example;

import java.io.InputStream;

import org.apache.helix.provisioning.ApplicationSpec;
import org.apache.helix.provisioning.ApplicationSpecFactory;
import org.yaml.snakeyaml.Yaml;

public class MyTaskAppSpecFactory implements ApplicationSpecFactory {

  @Override
  public ApplicationSpec fromYaml(InputStream inputstream) {
    return (ApplicationSpec) new Yaml().load(inputstream);
    // return data;
  }

  public static void main(String[] args) {

    Yaml yaml = new Yaml();
    InputStream resourceAsStream =
        ClassLoader.getSystemClassLoader().getResourceAsStream("job_runner_app_spec.yaml");
    MyTaskAppSpec spec = yaml.loadAs(resourceAsStream, MyTaskAppSpec.class);
    String dump = yaml.dump(spec);
    System.out.println(dump);
    System.out.println(spec.getServiceConfig("JobRunner").getStringField("num_containers", "1"));

  }
}
