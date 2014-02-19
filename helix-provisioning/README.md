Checkout helix provisioning branch
cd helix
mvn clean package -DskipTests
cd helix-provisioning


yyDownload and install YARN start all services (datanode, resourcemanage, nodemanager, jobHistoryServer(optional))

Will post the instructions to get a local YARN cluster.

target/helix-provisioning-pkg/bin/app-launcher.sh org.apache.helix.provisioning.yarn.example.HelloWordAppSpecFactory /Users/kgopalak/Documents/projects/incubator-helix/helix-provisioning/src/main/resources/hello_world_app_spec.yaml





