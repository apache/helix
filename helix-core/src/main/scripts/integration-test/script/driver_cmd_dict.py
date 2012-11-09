#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# this is the file to be included by dds_driver.py for product (e.g., espresso) specific setting
#
possible_ivy_dir=[os.path.join(os.environ["HOME"],".m2/repository"),os.path.join(os.environ["HOME"],".gradle/cache"),os.path.join(os.environ["HOME"],".ivy2/lin-cache/ivy-cache"),os.path.join(os.environ["HOME"],".ivy2/lin-cache"),"/ivy/.ivy2/ivy-cache","/ivy/.ivy2", os.path.join(os.environ["VIEW_ROOT"],"build/ivy2/cache")]
zookeeper_classpath="IVY_DIR/org/apache/zookeeper/zookeeper/3.3.3/zookeeper-3.3.3.jar:IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"

# espresso use -2
kill_cmd_template="jps | grep %s | cut -f1 -d\\  | xargs kill -2"
kill_container_template="ps -ef | grep tail | grep %s | awk '{print $2}' | xargs kill -9"

# This is not used in helix
afterParsingHook=None

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
    ,"cluster-manager":{"start":"ant -f cluster-manager/run/build.xml run-cluster-manager", "stop":kill_cmd_template % "HelixControllerMain"}
    ,"mock-storage":{"start":"ant -f cluster-manager/run/build.xml run-mock-storage", "stop":kill_cmd_template % "MockStorageProcess"}
    ,"cluster-state-verifier":{"start":"ant -d -f cluster-manager/run/build.xml run-cluster-state-verifier", "stop":kill_cmd_template % "ClusterStateVerifier"}
    ,"dummy-process":{"start":"ant -f cluster-manager/run/build.xml run-dummy-process", "stop":kill_cmd_template % "DummyProcess"}
    ,"mock-health-report-process":{"start":"ant -f cluster-manager/run/build.xml run-mock-health-report-process", "stop":kill_cmd_template % "MockHealthReportParticipant"}
    ,"clm_console":{"default":"ant -f cluster-manager/run/build.xml run-cm-console","stop":kill_cmd_template % "ClusterSetup"}
}

cmd_ret_pattern={    # the pattern when the call is considered return successfully
    "storage-node_start":re.compile("Espresso service started")
   ,"router_start":re.compile("Espresso service started")
   ,"cluster-manager_start":re.compile("No Messages to process")
   ,"mock-storage_start":re.compile("Mock storage started") 
   ,"dummy-process_start":re.compile("Dummy process started") 
   ,"mock-health-report-process_start":re.compile("MockHealthReportParticipant process started") 
}

# the mapping of option to the java options, if not give, then use directly
direct_java_call_option_mapping={
   "dump_file":"-f "
   ,"value_file":"--value.dump.file="
   #,"log4j_file":"-Dlog4j.configuration=file://"   # this is only for cluster manager
   #,"log4j_file":"--log_props="
   ,"config":"--container_props="
   ,"consumer_event_pattern":"event_pattern"
   ,"cmdline_props":"--cmdline_props="
   ,"cmdline_args":" "   # just put the cmdline_args directly
   ,"relay_host":"--relay_host="
   ,"relay_port":"--relay_port="
   #,"jmx_service_port":"--jmx_service_port="
   ,"bootstrap_host":"--bootstrap_host="
   ,"bootstrap_port":"--bootstrap_port="
   ,"http_port":"--http_port="
   ,"checkpoint_dir":"--checkpoint_dir="
   ,"dbname":"--dbname="
   ,"tablename":"--tablename="
   ,"dburi":"--dburi="
   ,"dbuser":"--dbuser="
   ,"dbpasswd":"--dbpassword="
   ,"schemareg":"--schemareg="
   ,"schemareg":"--schemareg="
   ,"db_relay_config":"--db_relay_config="
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
direct_java_call_jvm_args_ordered=[
   "jvm_direct_memory_size"
   ,"jvm_max_heap_size"
   ,"jvm_min_heap_size"
   ,"jvm_gc_log"
   ,"jvm_args"
   ,"log4j_file"
]
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
   ,"cmdline_props":"cmdline.props"
   ,"cmdline_args":"config.cmdline"
   ,"relay_host":"relay.host"
   ,"relay_port":"relay.port"
   #,"jmx_service_port":"jmx.service.port"
   ,"bootstrap_host":"bootstrap.host"
   ,"bootstrap_port":"bootstrap.port"
   ,"consumer_event_pattern":"consumer.event.pattern"
   ,"http_port":"http.port"
   ,"checkpoint_dir":"checkpoint.dir"
#   ,"db_relay_config":"db.relay.config" 
}

# class path
import glob
#print "view_root=" + get_view_root()
cm_jar_files=glob.glob(os.path.join(get_view_root(),"../../../target/helix-core-*.jar"))
#cm_jar_file=os.path.basename(cm_jar_file)
#print cm_jar_file
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
      ,"IVY_DIR/org/apache/zookeeper/zookeeper/3.3.3/zookeeper-3.3.3.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-core-asl/1.8.5/jackson-core-asl-1.8.5.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-mapper-asl/1.8.5/jackson-mapper-asl-1.8.5.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/org/restlet/org.restlet/1.1.10/org.restlet-1.1.10.jar"
      ,"IVY_DIR/com/noelios/restlet/com.noelios.restlet/1.1.10/com.noelios.restlet-1.1.10.jar"
]+cm_jar_files
  ,"class_name":"org.apache.helix.tools.ClusterSetup"
  ,"before_cmd":"../../../mvn package -Dmaven.test.skip.exec=true"  # build jar first
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
      ,"IVY_DIR/org/apache/zookeeper/zookeeper/3.3.3/zookeeper-3.3.3.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-core-asl/1.8.5/jackson-core-asl-1.8.5.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-mapper-asl/1.8.5/jackson-mapper-asl-1.8.5.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/com/github/sgroschupf/zkclient/0.1/zkclient-0.1.jar"
      ,"IVY_DIR/org/apache/commons/commons-math/2.1/commons-math-2.1.jar"
]+cm_jar_files

  ,"class_name":"org.apache.helix.mock.participant.DummyProcess"
  ,"before_cmd":"../../../mvn package -Dmaven.test.skip.exec=true"  # build jar first
   }

  ,"mock-health-report-process":
   {
    "class_path":[
      "IVY_DIR/com/thoughtworks/xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/org/apache/zookeeper/zookeeper/3.3.3/zookeeper-3.3.3.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-core-asl/1.8.5/jackson-core-asl-1.8.5.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-mapper-asl/1.8.5/jackson-mapper-asl-1.8.5.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/com/github/sgroschupf/zkclient/0.1/zkclient-0.1.jar"
      ,"IVY_DIR/org/apache/commons/commons-math/2.1/commons-math-2.1.jar"
      ,"IVY_DIR/org/restlet/org.restlet/1.1.10/org.restlet-1.1.10.jar"
      ,"IVY_DIR/com/noelios/restlet/com.noelios.restlet/1.1.10/com.noelios.restlet-1.1.10.jar"
]+cm_jar_files

  ,"class_name":"org.apache.helix.mock.participant.MockHealthReportParticipant"
  ,"before_cmd":"../../../mvn package -Dmaven.test.skip.exec=true"  # build jar first
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
      ,"IVY_DIR/org/apache/zookeeper/zookeeper/3.3.3/zookeeper-3.3.3.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-core-asl/1.8.5/jackson-core-asl-1.8.5.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-mapper-asl/1.8.5/jackson-mapper-asl-1.8.5.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/com/github/sgroschupf/zkclient/0.1/zkclient-0.1.jar"
      ,"IVY_DIR/org/apache/commons/commons-math/2.1/commons-math-2.1.jar"
      ,"IVY_DIR/org/restlet/org.restlet/1.1.10/org.restlet-1.1.10.jar"
      ,"IVY_DIR/com/noelios/restlet/com.noelios.restlet/1.1.10/com.noelios.restlet-1.1.10.jar"
]+cm_jar_files
  ,"class_name":"org.apache.helix.controller.HelixControllerMain"
  ,"before_cmd":"../../../mvn package -Dmaven.test.skip.exec=true"  # build jar first
   }

  ,"cluster-state-verifier":
   {
    "class_path":[
      "IVY_DIR/com/github/sgroschupf/zkclient/0.1/zkclient-0.1.jar"
      ,"IVY_DIR/com/thoughtworks/xstream/xstream/1.3.1/xstream-1.3.1.jar"
      ,"IVY_DIR/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
      ,"IVY_DIR/commons-io/commons-io/1.4/commons-io-1.4.jar"
      ,"IVY_DIR/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
      ,"IVY_DIR/jdom/jdom/1.0/jdom-1.0.jar"
      ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
      ,"IVY_DIR/org/apache/zookeeper/zookeeper/3.3.3/zookeeper-3.3.3.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-core-asl/1.8.5/jackson-core-asl-1.8.5.jar"
      ,"IVY_DIR/org/codehaus/jackson/jackson-mapper-asl/1.8.5/jackson-mapper-asl-1.8.5.jar"
      ,"IVY_DIR/xpp3/xpp3_min/1.1.4c/xpp3_min-1.1.4c.jar"
      ,"IVY_DIR/org/restlet/org.restlet/1.1.10/org.restlet-1.1.10.jar"
      ,"IVY_DIR/com/noelios/restlet/com.noelios.restlet/1.1.10/com.noelios.restlet-1.1.10.jar"
]+cm_jar_files
  ,"class_name":"org.apache.helix.tools.ClusterStateVerifier"
  ,"before_cmd":"../../../mvn package -Dmaven.test.skip.exec=true"  # build jar first
   }

  ,"mock-storage":
   {
    "class_path":[
      "IVY_DIR/com/github/sgroschupf/zkclient/0.1/zkclient-0.1.jar"
     ,"IVY_DIR/log4j/log4j/1.2.15/log4j-1.2.15.jar"
     ,"IVY_DIR/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
     ,"IVY_DIR/commons-math/commons-math/2.1/commons-math-2.1.jar"
]+cm_jar_files
  ,"class_name":"org.apache.helix.mock.participant.MockStorageProcess"
   }
}

# file the log4j file
def log4j_file_store_value(option, opt_str, value, parser):
  setattr(parser.values, option.dest, file_exists(value))
# configure
config_group = OptionGroup(parser, "Config options", "")
config_group.add_option("-p", "--config", action="store", dest="config", default=None,
                   help="config file path")
config_group.add_option("--dump_file", action="store", dest="dump_file", default=None,
                   help="Event dump file")
config_group.add_option("--value_file", action="store", dest="value_file", default=None,
                   help="Event value dump file")
config_group.add_option("-l", "--log4j_file", action="callback", callback=log4j_file_store_value, type="str", dest="log4j_file", default=None,
                   help="Log4j config file")
#config_group.add_option("-l", "--log4j_file", action="store", dest="log4j_file", default=None,
#                   help="Log4j config file")
config_group.add_option("--relay_host", action="store", dest="relay_host", default=None,
                   help="Host of relay for a consumer")
config_group.add_option("--relay_port", action="store", dest="relay_port", default=None,
                   help="Port of relay for a consumer")
config_group.add_option("--http_port", action="store", dest="http_port", default=None,
                   help="Http Port of the current started component")
config_group.add_option("--db_relay_config", action="store", dest="db_relay_config", default=None,
                   help="DB relay config file")
config_group.add_option("--cmdline_props", action="store", dest="cmdline_props", default=None,
                   help="Command line config props. Comma separate config parameter, e.g., --cmdline_props=databus.relay.eventBuffer.maxSize=1024000;...")
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
config_group.add_option("--dbname", action="store", dest="dbname", default=None, help="Espresso db name")
config_group.add_option("--tablename", action="store", dest="tablename", default=None, help="Espresso table name")
config_group.add_option("--dburi", action="store", dest="dburi", default=None, help="Espresso db uri")
config_group.add_option("--dbuser", action="store", dest="dbuser", default=None, help="Espresso db user")
config_group.add_option("--dbpasswd", action="store", dest="dbpasswd", default=None, help="Espresso db password")
config_group.add_option("--schemareg", action="store", dest="schemareg", default=None, help="Espresso schemareg ")
config_group.add_option("-x","--extservice_props", action="append", dest="extservice_props", default=None,
                       help="Config props to override the extservices. Can give multiple times. One for each property. <entry name>;<prop name>;value. e.g., databus2.relay.local.bizfollow;db.bizfollow.db_url;jdbc.. ")
config_group.add_option("--cmdline_args", action="store", dest="cmdline_args", default=None, help="Command line arguments")

other_option_group = OptionGroup(parser, "Other options", "")
other_option_group.add_option("", "--component_id", action="store", dest="component_id", default = None,
                   help="The compnent id (1,2..) if there are mutliple instance of a component")
parser.add_option("","--sleep_before_wait", action="store", type="long", dest="sleep_before_wait", default=0,
                   help="Sleep secs before waiting consumer reaching maxEventWindowScn. [default: %default]")
parser.add_option("","--sleep_after_wait", action="store", type="long", dest="sleep_after_wait", default=1,
                   help="Sleep secs after consumer reaching maxEventWindowScn. [default: %default]")
parser.add_option("","--producer_log_purge_limit", action="store", type="int", dest="producer_log_purge_limit", default=1000,
                   help="The limit on number of logs to purge for producer [default: %default]")

