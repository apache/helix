#cd ../../
#mvn clean install -DskipTests
#cd recipes/helloworld-provisioning-yarn
#mvn clean package -DskipTests
chmod +x target/helloworld-provisioning-yarn-pkg/bin/app-launcher.sh
target/helloworld-provisioning-yarn-pkg/bin/app-launcher.sh org.apache.helix.provisioning.yarn.example.HelloWordAppSpecFactory /Users/kgopalak/Documents/projects/incubator-helix/recipes/helloworld-provisioning-yarn/src/main/resources/hello_world_app_spec.yaml
