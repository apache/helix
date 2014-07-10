#cd ../../
#mvn clean install -DskipTests
#cd recipes/helloworld-provisioning-yarn
#mvn install package -DskipTests
chmod +x target/jobrunner-yarn-pkg/bin/app-launcher.sh
target/jobrunner-yarn-pkg/bin/app-launcher.sh --app_spec_provider org.apache.helix.provisioning.yarn.example.MyTaskAppSpecFactory --app_config_spec /Users/kbiscuit/helix/incubator-helix/recipes/jobrunner-yarn/src/main/resources/job_runner_app_spec.yaml
