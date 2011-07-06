# espresso use -2
kill_cmd_template="jps | grep %s | cut -f1 -d\\  | xargs kill -2"
kill_container_template="ps -ef | grep tail | grep %s | awk '{print $2}' | xargs kill -9"

# some global variables
router_http_port=12917
router_mgmt_port=12920
storage_node_http_port=12918
storage_node_mgmt_port=12919
curl_kill_cmd_template="curl -s http://localhost:%d/pid | xargs kill -2"

# used to run cmd, can combine multiple command
cmd_dict={
     "storage-node":{"start":"%s; %s" % (curl_kill_cmd_template % storage_node_mgmt_port,"ant -f espresso-storage-node/run/build.xml run-storage-node"),"stop":curl_kill_cmd_template % storage_node_mgmt_port,"stats":[get_stats,"EspressoSingleNode"]}
    ,"router":{"start":"ant -f espresso-router/run/build.xml run-router","stop":curl_kill_cmd_template % router_mgmt_port,"stats":[get_stats,"EspressoRouter"]}
    ,"zookeeper":{"start":[zookeeper_opers,"start"],"stop":[zookeeper_opers,"stop"],"wait_for_exist":[zookeeper_opers,"wait_for_exist"],"wait_for_nonexist":[zookeeper_opers,"wait_for_nonexist"],"wait_for_value":[zookeeper_opers,"wait_for_value"],"cmd":[zookeeper_opers,"cmd"]}
    ,"cluster-manager":{"start":"ant -f cluster-manager/run/build.xml run-cluster-manager", "stop":kill_cmd_template % "ClusterManagerMain"}
    ,"mock-storage":{"start":"ant -f cluster-manager/run/build.xml run-mock-storage", "stop":kill_cmd_template % "MockStorageProcess"}
    ,"cluster-state-verifier":{"start":"ant -d -f cluster-manager/run/build.xml run-cluster-state-verifier", "stop":kill_cmd_template % "ClusterStateVerifier"}
    ,"dummy-process":{"start":"ant -f cluster-manager/run/build.xml run-dummy-process", "stop":kill_cmd_template % "DummyProcess"}
    ,"clm_console":{"default":"ant -f cluster-manager/run/build.xml run-cm-console","stop":kill_cmd_template % "ClusterSetup"}
}

cmd_ret_pattern={    # the pattern when the call is considered return successfully
    "storage-node_start":re.compile("Espresso service started")
   ,"router_start":re.compile("Espresso service started")
   ,"cluster-manager_start":re.compile("Cluster manager started")
   ,"mock-storage_start":re.compile("Mock storage started") 
   ,"dummy-process_start":re.compile("Dummy process started") 
}

# the mapping of option to the java options, if not give, then use directly
direct_java_call_option_mapping={
   "dump_file":"-f "
   ,"value_file":"--value.dump.file="
   #,"log4j_file":"-Dlog4j.configuration=file://"   # this is only for cluster manager
   #,"log4j_file":"--log_props="
   ,"config_file":"--container_props="
   ,"consumer_event_pattern":"event_pattern"
   ,"db_config_file":"--db_config_file="
   ,"cmdline_props":"--cmdline_props="
   ,"cmdline_args":" "   # just put the cmdline_args directly
   ,"filter_conf_file":"--filter_conf_file=" 
   ,"relay_host":"--relay_host="
   ,"relay_port":"--relay_port="
   ,"jmx_service_port":"--jmx_service_port="
   ,"bootstrap_host":"--bootstrap_host="
   ,"bootstrap_port":"--bootstrap_port="
   ,"http_port":"--http_port="
   ,"checkpoint_dir":"--checkpoint_dir="
}
# has default value, append to the beginning
direct_java_call_jvm_args={
   "jvm_direct_memory_size":["-XX:MaxDirectMemorySize=","100m"]
   ,"jvm_max_heap_size":["-Xmx","512m"]
   ,"jvm_min_heap_size":["-Xms","100m"]
   ,"jvm_gc_log":["-Xloggc:",""]
   ,"jvm_args":["",""] 
   ,"log4j_file":["-Dlog4j.configuration=file://",""]   # this is only for cluster manager
}

# mapping from option to ant
ant_call_option_mapping={
   "dump_file":"dump.file"
   ,"value_file":"value.dump.file"
   ,"log4j_file":"log4j.file"
   ,"config":"config.file"
   ,"jvm_direct_memory_size":"jvm.direct.memory.size"
   ,"jvm_max_heap_size":"jvm.max.heap.size"
   ,"jvm_gc_log":"jvm.gc.log" 
   ,"jvm_args":"jvm.args" 
   ,"db_config_file":"db.relay.config" 
   ,"cmdline_props":"cmdline.props"
   ,"cmdline_args":"config.cmdline"
   ,"filter_conf_file":"filter.conf.file" 
   ,"relay_host":"relay.host"
   ,"relay_port":"relay.port"
   ,"jmx_service_port":"jmx.service.port"
   ,"bootstrap_host":"bootstrap.host"
   ,"bootstrap_port":"bootstrap.port"
   ,"consumer_event_pattern":"consumer.event.pattern"
   ,"http_port":"http.port"
   ,"checkpoint_dir":"checkpoint.dir"
}
 
 
# class path
cmd_direct_call={
   "clm_console":
   {
    "class_path":[
      "IVY_DIR/com/github/sgroschupf/zkclient/0.1/zkclient-0.1.jar"
      ,"IVY_DIR/com/thoughtworks/xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/org/apache/zookeeper/zookeeper/3.3.0/zookeeper-3.3.0.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-core-asl/1.4.2/jackson-core-asl-1.4.2.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-mapper-asl/1.4.2/jackson-mapper-asl-1.4.2.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"VIEW_ROOT/target/cluster-manager-core-0.0.1.jar"
]
  ,"class_name":"com.linkedin.clustermanager.tools.ClusterSetup"
   }

  ,"dummy-process":
   {
    "class_path":[
      "IVY_DIR/com/thoughtworks/xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/org/apache/zookeeper/zookeeper/3.3.0/zookeeper-3.3.0.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-core-asl/1.4.2/jackson-core-asl-1.4.2.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-mapper-asl/1.4.2/jackson-mapper-asl-1.4.2.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/com/github/sgroschupf/zkclient/0.1/zkclient-0.1.jar"
      ,"VIEW_ROOT/target/cluster-manager-core-0.0.1.jar"
]
  ,"class_name":"com.linkedin.clustermanager.mock.storage.DummyProcess"
   }

  ,"cluster-manager":
   {
    "class_path":[
      "IVY_DIR/com/thoughtworks/xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/org/apache/zookeeper/zookeeper/3.3.0/zookeeper-3.3.0.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-core-asl/1.4.2/jackson-core-asl-1.4.2.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-mapper-asl/1.4.2/jackson-mapper-asl-1.4.2.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/com/github/sgroschupf/zkclient/0.1/zkclient-0.1.jar"
      ,"VIEW_ROOT/target/cluster-manager-core-0.0.1.jar"
]
  ,"class_name":"com.linkedin.clustermanager.controller.ClusterManagerMain"
   }

  ,"mock-storage":
   {
    "class_path":[
      "IVY_DIR/cglib/cglib-nodep/2.2/cglib-nodep-2.2.jar"
      ,"IVY_DIR/com.linkedin.avro-schema-tools/avro-schema-tools/0.1.3/avro-schema-tools-0.1.3.jar"
      ,"IVY_DIR/com.linkedin.cfg2.cfg/cfg-api/2.1.0/cfg-api-2.1.0.jar"
      ,"IVY_DIR/com.linkedin.cfg2.cfg/cfg-impl/2.1.0/cfg-impl-2.1.0.jar"
      ,"IVY_DIR/com.linkedin.network.configuration/configuration-repository-impl/0.0.1114-RC1.BR_REL_1114_177600/configuration-repository-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-eventbus-api/0.0.1114-RC1.BR_REL_1114_177600/container-eventbus-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-eventbus-impl/0.0.1114-RC1.BR_REL_1114_177600/container-eventbus-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-http-impl/0.0.1114-RC1.BR_REL_1114_177600/container-http-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-ic-api/0.0.1114-RC1.BR_REL_1114_177600/container-ic-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-ic-impl/0.0.1114-RC1.BR_REL_1114_177600/container-ic-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-jmx-impl/0.0.1114-RC1.BR_REL_1114_177600/container-jmx-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-rpc-trace-api/0.0.1114-RC1.BR_REL_1114_177600/container-rpc-trace-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-rpc-trace-impl/0.0.1114-RC1.BR_REL_1114_177600/container-rpc-trace-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-api/0.0.1114-RC1.BR_REL_1114_177600/dal-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-common-impl/0.0.1114-RC1.BR_REL_1114_177600/dal-common-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-drc-api/0.0.1114-RC1.BR_REL_1114_177600/dal-drc-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-drc-impl/0.0.1114-RC1.BR_REL_1114_177600/dal-drc-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-dsc-api/0.0.1114-RC1.BR_REL_1114_177600/dal-dsc-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-dsc-impl/0.0.1114-RC1.BR_REL_1114_177600/dal-dsc-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.healthcheck/healthcheck-api/0.0.1114-RC1.BR_REL_1114_177600/healthcheck-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.lispring/lispring-lispring-core/0.0.1114-RC1.BR_REL_1114_177600/lispring-lispring-core-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.lispring/lispring-lispring-servlet/0.0.1114-RC1.BR_REL_1114_177600/lispring-lispring-servlet-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.security/security-api/0.0.1114-RC1.BR_REL_1114_177600/security-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.util/util-core/1.0.15/util-core-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-i18n/1.0.15/util-i18n-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-log/1.0.15/util-log-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-servlet/1.0.15/util-servlet-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-xmsg/1.0.15/util-xmsg-1.0.15.jar"
      ,"IVY_DIR/com.thoughtworks.xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar"
      ,"IVY_DIR/commons-cli/commons-cli/1.0/commons-cli-1.0.jar"
      ,"IVY_DIR/commons-codec/commons-codec/1.3/commons-codec-1.3.jar"
      ,"IVY_DIR/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/commons-logging/commons-logging/1.1/commons-logging-1.1.jar"
      ,"IVY_DIR/dom4j/dom4j/1.6.1/dom4j-1.6.1.jar"
      ,"IVY_DIR/javax.activation/activation/1.0.2/activation-1.0.2.jar"
      ,"IVY_DIR/javax.j2ee/com.linkedin.customlibrary.j2ee/1.0/com.linkedin.customlibrary.j2ee-1.0.jar"
      ,"IVY_DIR/javax.mail/mail/1.3.0/mail-1.3.0.jar"
      ,"IVY_DIR/javax.servlet/servlet-api/2.5/servlet-api-2.5.jar"
      ,"IVY_DIR/jaxen/jaxen/1.0-FCS/jaxen-1.0-FCS.jar"
      ,"IVY_DIR/jaxen/saxpath/1.0-FCS/saxpath-1.0-FCS.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/joda-time/joda-time/1.6/joda-time-1.6.jar"
      ,"IVY_DIR/junit/junit/4.8.1/junit-4.8.1.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/mx4j/com.linkedin.customlibrary.mx4j/3.0.2/com.linkedin.customlibrary.mx4j-3.0.2.jar"
      ,"IVY_DIR/mx4j/mx4j-tools/3.0.2/mx4j-tools-3.0.2.jar"
      ,"IVY_DIR/net.java.dev.msg/com.linkedin.customlibrary.xmsg/0.51/com.linkedin.customlibrary.xmsg-0.51.jar"
      ,"IVY_DIR/org.apache.avro/avro/1.4.0/avro-1.4.0.jar"
      ,"IVY_DIR/org.apache.zookeeper/zookeeper/3.3.0/zookeeper-3.3.0.jar"
      ,"IVY_DIR/org.codehaus.jackson/jackson-core-asl/1.4.2/jackson-core-asl-1.4.2.jar"
      ,"IVY_DIR/org.codehaus.jackson/jackson-mapper-asl/1.4.2/jackson-mapper-asl-1.4.2.jar"
      ,"IVY_DIR/org.json/json-simple/1.1/json-simple-1.1.jar"
      ,"IVY_DIR/org.json/json/20070829/json-20070829.jar"
      ,"IVY_DIR/org.springframework/spring-webmvc/2.5.5/spring-webmvc-2.5.5.jar"
      ,"IVY_DIR/org.springframework/spring/2.5.5/spring-2.5.5.jar"
      ,"IVY_DIR/xml-apis/xml-apis/1.3.04/xml-apis-1.3.04.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/zkclient/zkclient/0.1.0/zkclient-0.1.0.jar"
      ,"VIEW_ROOT/build/cluster-manager/impl/lib/cluster-manager-impl.jar"
]
  ,"class_name":"com.linkedin.espresso.cm.mock.storage.MockStorageProcess"
   }
}

# configure
config_group = OptionGroup(parser, "Config options", "")
config_group.add_option("-p", "--config", action="store", dest="config", default=None,
                   help="config file path")
config_group.add_option("--dump_file", action="store", dest="dump_file", default=None,
                   help="Event dump file")
config_group.add_option("--value_file", action="store", dest="value_file", default=None,
                   help="Event value dump file")
config_group.add_option("-l", "--log4j_file", action="store", dest="log4j_file", default=None,
                   help="Log4j config file")
config_group.add_option("--relay_host", action="store", dest="relay_host", default=None,
                   help="Host of relay for a consumer")
config_group.add_option("--relay_port", action="store", dest="relay_port", default=None,
                   help="Port of relay for a consumer")
config_group.add_option("--http_port", action="store", dest="http_port", default=None,
                   help="Http Port of the current started component")
config_group.add_option("--jmx_service_port", action="store", dest="jmx_service_port", default=None,
                   help="JMX Service port")
config_group.add_option("--db_config_file", action="store", dest="db_config_file", default=None,
                   help="DB relay config file")
config_group.add_option("--cmdline_props", action="store", dest="cmdline_props", default=None,
                   help="Command line config props. Comma separate config parameter, e.g., --cmdline_props=databus.relay.eventBuffer.maxSize=1024000;...")
config_group.add_option("--cmdline_args", action="store", dest="cmdline_args", default=None,
                   help="Command line arguments")
config_group.add_option("--filter_conf_file", action="store", dest="filter_conf_file", default=None,
                   help="Filter conf file")
config_group.add_option("--bootstrap_host", action="store", dest="bootstrap_host", default=None,
                   help="Host of bootstrap server")
config_group.add_option("--bootstrap_port", action="store", dest="bootstrap_port", default=None,
                   help="Port of bootstrap server")
config_group.add_option("--checkpoint_dir", action="store", dest="checkpoint_dir", default=None,
                   help="Client checkpoint dir")
config_group.add_option("--checkpoint_keep", action="store_true", dest="checkpoint_keep", default=False,
                   help="Do NOT clean client checkpoint dir")
config_group.add_option("--consumer_event_pattern", action="store", dest="consumer_event_pattern", default=None,
                   help="Check consumer event pattern if set")
config_group.add_option("-x","--extservice_props", action="append", dest="extservice_props", default=None,
                   help="Config props to override the extservices. Can give multiple times. One for each property. <entry name>:<prop name>:value. e.g., databus2.relay.local.bizfollow: db.bizfollow.db_url ")
other_option_group = OptionGroup(parser, "Other options", "")
other_option_group.add_option("", "--component_id", action="store", dest="component_id", default = None,
                   help="The compnent id (1,2..) if there are mutliple instance of a component")
parser.add_option("","--sleep_before_wait", action="store", type="long", dest="sleep_before_wait", default=0,
                   help="Sleep secs before waiting consumer reaching maxEventWindowScn. [default: %default]")
parser.add_option("","--sleep_after_wait", action="store", type="long", dest="sleep_after_wait", default=1,
                   help="Sleep secs after consumer reaching maxEventWindowScn. [default: %default]")
parser.add_option("","--producer_log_purge_limit", action="store", type="int", dest="producer_log_purge_limit", default=1000,
                   help="The limit on number of logs to purge for producer [default: %default]")

#####
'''
  "clm_console":
   {
    "class_path":[
      "IVY_DIR/cglib/cglib-nodep/2.2/cglib-nodep-2.2.jar"
      ,"IVY_DIR/com.linkedin.avro-schema-tools/avro-schema-tools/0.1.3/avro-schema-tools-0.1.3.jar"
      ,"IVY_DIR/com.linkedin.cfg2.cfg/cfg-api/2.1.0/cfg-api-2.1.0.jar"
      ,"IVY_DIR/com.linkedin.cfg2.cfg/cfg-impl/2.1.0/cfg-impl-2.1.0.jar"
      ,"IVY_DIR/com.linkedin.network.configuration/configuration-repository-impl/0.0.1114-RC1.BR_REL_1114_177600/configuration-repository-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-eventbus-api/0.0.1114-RC1.BR_REL_1114_177600/container-eventbus-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-eventbus-impl/0.0.1114-RC1.BR_REL_1114_177600/container-eventbus-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-http-impl/0.0.1114-RC1.BR_REL_1114_177600/container-http-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-ic-api/0.0.1114-RC1.BR_REL_1114_177600/container-ic-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-ic-impl/0.0.1114-RC1.BR_REL_1114_177600/container-ic-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-jmx-impl/0.0.1114-RC1.BR_REL_1114_177600/container-jmx-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-rpc-trace-api/0.0.1114-RC1.BR_REL_1114_177600/container-rpc-trace-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-rpc-trace-impl/0.0.1114-RC1.BR_REL_1114_177600/container-rpc-trace-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-api/0.0.1114-RC1.BR_REL_1114_177600/dal-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-common-impl/0.0.1114-RC1.BR_REL_1114_177600/dal-common-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-drc-api/0.0.1114-RC1.BR_REL_1114_177600/dal-drc-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-drc-impl/0.0.1114-RC1.BR_REL_1114_177600/dal-drc-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-dsc-api/0.0.1114-RC1.BR_REL_1114_177600/dal-dsc-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-dsc-impl/0.0.1114-RC1.BR_REL_1114_177600/dal-dsc-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.healthcheck/healthcheck-api/0.0.1114-RC1.BR_REL_1114_177600/healthcheck-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.lispring/lispring-lispring-core/0.0.1114-RC1.BR_REL_1114_177600/lispring-lispring-core-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.lispring/lispring-lispring-servlet/0.0.1114-RC1.BR_REL_1114_177600/lispring-lispring-servlet-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.security/security-api/0.0.1114-RC1.BR_REL_1114_177600/security-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.util/util-core/1.0.15/util-core-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-i18n/1.0.15/util-i18n-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-log/1.0.15/util-log-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-servlet/1.0.15/util-servlet-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-xmsg/1.0.15/util-xmsg-1.0.15.jar"
      ,"IVY_DIR/com.thoughtworks.xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar"
      ,"IVY_DIR/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
      ,"IVY_DIR/commons-codec/commons-codec/1.3/commons-codec-1.3.jar"
      ,"IVY_DIR/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/commons-logging/commons-logging/1.1/commons-logging-1.1.jar"
      ,"IVY_DIR/dom4j/dom4j/1.6.1/dom4j-1.6.1.jar"
      ,"IVY_DIR/javax.activation/activation/1.0.2/activation-1.0.2.jar"
      ,"IVY_DIR/javax.j2ee/com.linkedin.customlibrary.j2ee/1.0/com.linkedin.customlibrary.j2ee-1.0.jar"
      ,"IVY_DIR/javax.mail/mail/1.3.0/mail-1.3.0.jar"
      ,"IVY_DIR/javax.servlet/servlet-api/2.5/servlet-api-2.5.jar"
      ,"IVY_DIR/jaxen/jaxen/1.0-FCS/jaxen-1.0-FCS.jar"
      ,"IVY_DIR/jaxen/saxpath/1.0-FCS/saxpath-1.0-FCS.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/joda-time/joda-time/1.6/joda-time-1.6.jar"
      ,"IVY_DIR/junit/junit/4.8.1/junit-4.8.1.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/mx4j/com.linkedin.customlibrary.mx4j/3.0.2/com.linkedin.customlibrary.mx4j-3.0.2.jar"
      ,"IVY_DIR/mx4j/mx4j-tools/3.0.2/mx4j-tools-3.0.2.jar"
      ,"IVY_DIR/net.java.dev.msg/com.linkedin.customlibrary.xmsg/0.51/com.linkedin.customlibrary.xmsg-0.51.jar"
      ,"IVY_DIR/org.apache.avro/avro/1.4.0/avro-1.4.0.jar"
      ,"IVY_DIR/org.apache.zookeeper/zookeeper/3.3.0/zookeeper-3.3.0.jar"
      ,"IVY_DIR/org.codehaus.jackson/jackson-core-asl/1.4.2/jackson-core-asl-1.4.2.jar"
      ,"IVY_DIR/org.codehaus.jackson/jackson-mapper-asl/1.4.2/jackson-mapper-asl-1.4.2.jar"
      ,"IVY_DIR/org.json/json-simple/1.1/json-simple-1.1.jar"
      ,"IVY_DIR/org.json/json/20070829/json-20070829.jar"
      ,"IVY_DIR/org.springframework/spring-webmvc/2.5.5/spring-webmvc-2.5.5.jar"
      ,"IVY_DIR/org.springframework/spring/2.5.5/spring-2.5.5.jar"
      ,"IVY_DIR/xml-apis/xml-apis/1.3.04/xml-apis-1.3.04.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      #,"IVY_DIR/zkclient/zkclient/0.1.0/zkclient-0.1.0.jar"
      ,"IVY_DIR/com/github/sgroschupf/zkclient/0.1/zkclient-0.1.jar"
      #,"VIEW_ROOT/build/cluster-manager/impl/lib/cluster-manager-impl.jar"
      ,"VIEW_ROOT/target/cluster-manager-core-1.0-SNAPSHOT.jar"
]
#/Users/dzhang/project/cluster-manager/cluster-manager-core/target//cluster-manager-core-1.0-SNAPSHOT.jar
  ,"class_name":"com.linkedin.clustermanager.tools.ClusterSetup"
   }

  ,"dummy-process":
   {
    "class_path":[
      "IVY_DIR/cglib/cglib-nodep/2.2/cglib-nodep-2.2.jar"
      ,"IVY_DIR/com.linkedin.avro-schema-tools/avro-schema-tools/0.1.3/avro-schema-tools-0.1.3.jar"
      ,"IVY_DIR/com.linkedin.cfg2.cfg/cfg-api/2.1.0/cfg-api-2.1.0.jar"
      ,"IVY_DIR/com.linkedin.cfg2.cfg/cfg-impl/2.1.0/cfg-impl-2.1.0.jar"
      ,"IVY_DIR/com.linkedin.network.configuration/configuration-repository-impl/0.0.1114-RC1.BR_REL_1114_177600/configuration-repository-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-eventbus-api/0.0.1114-RC1.BR_REL_1114_177600/container-eventbus-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-eventbus-impl/0.0.1114-RC1.BR_REL_1114_177600/container-eventbus-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-http-impl/0.0.1114-RC1.BR_REL_1114_177600/container-http-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-ic-api/0.0.1114-RC1.BR_REL_1114_177600/container-ic-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-ic-impl/0.0.1114-RC1.BR_REL_1114_177600/container-ic-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-jmx-impl/0.0.1114-RC1.BR_REL_1114_177600/container-jmx-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-rpc-trace-api/0.0.1114-RC1.BR_REL_1114_177600/container-rpc-trace-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-rpc-trace-impl/0.0.1114-RC1.BR_REL_1114_177600/container-rpc-trace-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-api/0.0.1114-RC1.BR_REL_1114_177600/dal-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-common-impl/0.0.1114-RC1.BR_REL_1114_177600/dal-common-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-drc-api/0.0.1114-RC1.BR_REL_1114_177600/dal-drc-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-drc-impl/0.0.1114-RC1.BR_REL_1114_177600/dal-drc-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-dsc-api/0.0.1114-RC1.BR_REL_1114_177600/dal-dsc-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-dsc-impl/0.0.1114-RC1.BR_REL_1114_177600/dal-dsc-impl-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.healthcheck/healthcheck-api/0.0.1114-RC1.BR_REL_1114_177600/healthcheck-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.lispring/lispring-lispring-core/0.0.1114-RC1.BR_REL_1114_177600/lispring-lispring-core-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.lispring/lispring-lispring-servlet/0.0.1114-RC1.BR_REL_1114_177600/lispring-lispring-servlet-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.network.security/security-api/0.0.1114-RC1.BR_REL_1114_177600/security-api-0.0.1114-RC1.BR_REL_1114_177600.jar"
      ,"IVY_DIR/com.linkedin.util/util-core/1.0.15/util-core-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-i18n/1.0.15/util-i18n-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-log/1.0.15/util-log-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-servlet/1.0.15/util-servlet-1.0.15.jar"
      ,"IVY_DIR/com.linkedin.util/util-xmsg/1.0.15/util-xmsg-1.0.15.jar"
      ,"IVY_DIR/com.thoughtworks.xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar"
      ,"IVY_DIR/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
      #,"IVY_DIR/commons-cli/commons-cli/1.0/commons-cli-1.0.jar"
      ,"IVY_DIR/commons-codec/commons-codec/1.3/commons-codec-1.3.jar"
      ,"IVY_DIR/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/commons-logging/commons-logging/1.1/commons-logging-1.1.jar"
      ,"IVY_DIR/dom4j/dom4j/1.6.1/dom4j-1.6.1.jar"
      ,"IVY_DIR/javax.activation/activation/1.0.2/activation-1.0.2.jar"
      ,"IVY_DIR/javax.j2ee/com.linkedin.customlibrary.j2ee/1.0/com.linkedin.customlibrary.j2ee-1.0.jar"
      ,"IVY_DIR/javax.mail/mail/1.3.0/mail-1.3.0.jar"
      ,"IVY_DIR/javax.servlet/servlet-api/2.5/servlet-api-2.5.jar"
      ,"IVY_DIR/jaxen/jaxen/1.0-FCS/jaxen-1.0-FCS.jar"
      ,"IVY_DIR/jaxen/saxpath/1.0-FCS/saxpath-1.0-FCS.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/joda-time/joda-time/1.6/joda-time-1.6.jar"
      ,"IVY_DIR/junit/junit/4.8.1/junit-4.8.1.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/mx4j/com.linkedin.customlibrary.mx4j/3.0.2/com.linkedin.customlibrary.mx4j-3.0.2.jar"
      ,"IVY_DIR/mx4j/mx4j-tools/3.0.2/mx4j-tools-3.0.2.jar"
      ,"IVY_DIR/net.java.dev.msg/com.linkedin.customlibrary.xmsg/0.51/com.linkedin.customlibrary.xmsg-0.51.jar"
      ,"IVY_DIR/org.apache.avro/avro/1.4.0/avro-1.4.0.jar"
      ,"IVY_DIR/org.apache.zookeeper/zookeeper/3.3.0/zookeeper-3.3.0.jar"
      ,"IVY_DIR/org.codehaus.jackson/jackson-core-asl/1.4.2/jackson-core-asl-1.4.2.jar"
      ,"IVY_DIR/org.codehaus.jackson/jackson-mapper-asl/1.4.2/jackson-mapper-asl-1.4.2.jar"
      ,"IVY_DIR/org.json/json-simple/1.1/json-simple-1.1.jar"
      ,"IVY_DIR/org.json/json/20070829/json-20070829.jar"
      ,"IVY_DIR/org.springframework/spring-webmvc/2.5.5/spring-webmvc-2.5.5.jar"
      ,"IVY_DIR/org.springframework/spring/2.5.5/spring-2.5.5.jar"
      ,"IVY_DIR/xml-apis/xml-apis/1.3.04/xml-apis-1.3.04.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/zkclient/zkclient/0.1.0/zkclient-0.1.0.jar"
      ,"VIEW_ROOT/target/cluster-manager-core-1.0-SNAPSHOT.jar"
      #,"VIEW_ROOT/build/cluster-manager/impl/lib/cluster-manager-impl.jar"
]
  ,"class_name":"com.linkedin.clustermanager.mock.storage.DummyProcess"
   }

  ,"cluster-manager":
   {
    "class_path":[
      "IVY_DIR/com/thoughtworks/xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/org/apache/zookeeper/zookeeper/3.3.0/zookeeper-3.3.0.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-core-asl/1.4.2/jackson-core-asl-1.4.2.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-mapper-asl/1.4.2/jackson-mapper-asl-1.4.2.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"VIEW_ROOT/target/cluster-manager-core-1.0-SNAPSHOT.jar"
]
  ,"class_name":"com.linkedin.clustermanager.controller.ClusterManagerMain"
   }
  ,"cluster-manager":
   {
    "class_path":[
      "IVY_DIR/cglib/cglib-nodep/2.2/cglib-nodep-2.2.jar"
      ,"IVY_DIR/com.linkedin.avro-schema-tools/avro-schema-tools/0.1.3/avro-schema-tools-0.1.3.jar"
      ,"IVY_DIR/com.linkedin.cfg2.cfg/cfg-api/2.1.0/cfg-api-2.1.0.jar"
      ,"IVY_DIR/com.linkedin.cfg2.cfg/cfg-impl/2.1.0/cfg-impl-2.1.0.jar"
      ,"IVY_DIR/com.linkedin.network.configuration/configuration-repository-impl/0.0.1118-RC2.2374/configuration-repository-impl-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-eventbus-api/0.0.1118-RC2.2374/container-eventbus-api-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-eventbus-impl/0.0.1118-RC2.2374/container-eventbus-impl-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-http-impl/0.0.1118-RC2.2374/container-http-impl-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-ic-api/0.0.1118-RC2.2374/container-ic-api-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-ic-impl/0.0.1118-RC2.2374/container-ic-impl-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-jmx-impl/0.0.1118-RC2.2374/container-jmx-impl-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-rpc-trace-api/0.0.1118-RC2.2374/container-rpc-trace-api-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.container/container-rpc-trace-impl/0.0.1118-RC2.2374/container-rpc-trace-impl-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-api/0.0.1118-RC2.2374/dal-api-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-common-impl/0.0.1118-RC2.2374/dal-common-impl-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-drc-api/0.0.1118-RC2.2374/dal-drc-api-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-drc-impl/0.0.1118-RC2.2374/dal-drc-impl-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-dsc-api/0.0.1118-RC2.2374/dal-dsc-api-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.dal/dal-dsc-impl/0.0.1118-RC2.2374/dal-dsc-impl-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.healthcheck/healthcheck-api/0.0.1118-RC2.2374/healthcheck-api-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.lispring/lispring-lispring-core/0.0.1118-RC2.2374/lispring-lispring-core-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.lispring/lispring-lispring-servlet/0.0.1118-RC2.2374/lispring-lispring-servlet-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.network.security/security-api/0.0.1118-RC2.2374/security-api-0.0.1118-RC2.2374.jar"
      ,"IVY_DIR/com.linkedin.util/util-core/1.0.37/util-core-1.0.37.jar"
      ,"IVY_DIR/com.linkedin.util/util-i18n/1.0.37/util-i18n-1.0.37.jar"
      ,"IVY_DIR/com.linkedin.util/util-log/1.0.34/util-log-1.0.34.jar"
      ,"IVY_DIR/com.linkedin.util/util-servlet/1.0.34/util-servlet-1.0.34.jar"
      ,"IVY_DIR/com.linkedin.util/util-xmsg/1.0.37/util-xmsg-1.0.37.jar"
      ,"IVY_DIR/com.thoughtworks.xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar"
      ,"IVY_DIR/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
      ,"IVY_DIR/commons-codec/commons-codec/1.3/commons-codec-1.3.jar"
      ,"IVY_DIR/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/commons-logging/commons-logging/1.1/commons-logging-1.1.jar"
      ,"IVY_DIR/dom4j/dom4j/1.6.1/dom4j-1.6.1.jar"
      ,"IVY_DIR/javax.activation/activation/1.0.2/activation-1.0.2.jar"
      ,"IVY_DIR/javax.j2ee/com.linkedin.customlibrary.j2ee/1.0/com.linkedin.customlibrary.j2ee-1.0.jar"
      ,"IVY_DIR/javax.mail/mail/1.3.0/mail-1.3.0.jar"
      ,"IVY_DIR/javax.servlet/servlet-api/2.5/servlet-api-2.5.jar"
      ,"IVY_DIR/jaxen/jaxen/1.0-FCS/jaxen-1.0-FCS.jar"
      ,"IVY_DIR/jaxen/saxpath/1.0-FCS/saxpath-1.0-FCS.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/joda-time/joda-time/1.6/joda-time-1.6.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/mx4j/com.linkedin.customlibrary.mx4j/3.0.2/com.linkedin.customlibrary.mx4j-3.0.2.jar"
      ,"IVY_DIR/mx4j/mx4j-tools/3.0.2/mx4j-tools-3.0.2.jar"
      ,"IVY_DIR/net.java.dev.msg/com.linkedin.customlibrary.xmsg/0.51/com.linkedin.customlibrary.xmsg-0.51.jar"
      ,"IVY_DIR/org.apache.avro/avro/1.4.0/avro-1.4.0.jar"
      ,"IVY_DIR/org.apache.zookeeper/zookeeper/3.3.0/zookeeper-3.3.0.jar"
      ,"IVY_DIR/org.codehaus.jackson/jackson-core-asl/1.4.2/jackson-core-asl-1.4.2.jar"
      ,"IVY_DIR/org.codehaus.jackson/jackson-mapper-asl/1.4.2/jackson-mapper-asl-1.4.2.jar"
      ,"IVY_DIR/org.json/json-simple/1.1/json-simple-1.1.jar"
      ,"IVY_DIR/org.json/json/20070829/json-20070829.jar"
      ,"IVY_DIR/org.springframework/spring-webmvc/2.5.5/spring-webmvc-2.5.5.jar"
      ,"IVY_DIR/org.springframework/spring/2.5.5/spring-2.5.5.jar"
      ,"IVY_DIR/xml-apis/xml-apis/1.3.04/xml-apis-1.3.04.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/zkclient/zkclient/0.1.0/zkclient-0.1.0.jar"
      ,"VIEW_ROOT/target/cluster-manager-core-1.0-SNAPSHOT.jar"
]
  ,"class_name":"com.linkedin.clustermanager.controller.ClusterManagerMain"
   }


'''

