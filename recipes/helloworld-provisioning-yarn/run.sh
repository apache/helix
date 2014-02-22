cd ../../../../
mvn clean install -DskipTests
cd recipes/provisioning/yarn/helloworld/
mvn clean package -DskipTests
chmod +x target/helloworld-pkg/bin/app-launcher.sh
target/helloworld-pkg/bin/app-launcher.sh org.apache.helix.provisioning.yarn.example.HelloWordAppSpecFactory /Users/kgopalak/Documents/projects/incubator-helix/recipes/provisioning/yarn/helloworld/src/main/resources/hello_world_app_spec.yaml
