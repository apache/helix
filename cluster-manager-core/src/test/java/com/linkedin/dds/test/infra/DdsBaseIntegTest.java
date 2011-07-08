package com.linkedin.dds.test.infra;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;

public class DdsBaseIntegTest
{
  public final static String MODULE = DdsBaseIntegTest.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  // 2 minutes execution timeout
  public static final long EXEC_TIMEOUT = 120/*000*/;
  public static final String DDS_PRJ_BASED_DIR_TOKEN = "dds.basedir.path";
  public static final String DEFAULT_DDS_PRJ_BASED_DIR = "./";

  public static final String INTEGRATION_SCRIPT_DIR = "integration-test/script";
  public static final String DRIVER_SCRIPT_NAME = "cm_driver.py";
  public static final String TESTNAME_OPTION_STR = "-n";
  public static final String DEFAULT_TESTNAME = "default";
  public static final String COMPONENT_OPTION_STR = "-c";
  public static final String RELAY_SERVICE_COMPONENT = "test_relay";
  public static final String BOOTSTRAP_SERVICE_COMPONENT = "bootstrap_server";
  public static final String BOOTSTRAP_PRODUCER_COMPONENT = "test_bootstrap_producer";
  public static final String REINIT_COMPONENT = "bootstrap_dbreset";

  public static final String SERVICE_OPERATION_OPTION_STR = "-o";
  public static final String SERVICE_OPERATION_START = "start";
  public static final String SERVICE_OPERATION_STOP = "stop";

  public static final String RELAY_PROPERTY_OPTION_STR = "-p";
  public static final String RELAY_PROPERTY_NAME = "integration-test/config/test_integ_relay.properties";
  public static final String SMALL_BUFFER_RELAY_PROPERTY_NAME = "integration-test/config/small_integ_relay.properties";

  public static final String BOOTSTRAP_PROPERTY_OPTION_STR = "-p";
  public static final String BOOTSTRAP_PROPERTY_NAME = "config/bootstrap-service-config.properties";
  public static final String SMALL_FETCHSIZE_BOOTSTRAP_PROPERTY_NAME
    = "integration-test/config/small-fetchsize-bootstrap-service-config.properties";

  public static final String BOOTSTRAP_PRODUCER_OPTION_STR = "-p";
  public static final String BOOTSTRAP_PRODUCER_PROPERTY_NAME = "config/bootstrap-producer-config.properties";
  public static final String SMALL_LOG_BOOTSTAP_PRODUCER_PROPERTY_NAME
    = "integration-test/config/small-log-producer-config.properties";


  public static final String WORKLOAD_GEN_SCRIPT = "dbus2_gen_event.py";
  public static final String FROM_SCN_OPTION_STR = "--from_scn";
  public static final String EVENT_FREQUENCY_OPTION_STR = "--event_per_sec";
  public static final String DURATION_OPTION_STR = "--duration";
  public static final String NUMBER_OF_EVENTS_OPTION_STR = "--num_events";
  public static final String SOURCES_LIST_OPTION_STR = "--src_ids";
  public static final String PAUSE_WORKLOAD_OPTION_STR = "--suspend_gen";
  public static final String RESUME_WORKLOAD_OPTION_STR = "--resume_gen";
  public static final String WAIT_TIL_SUSPEND_OPTION_STR = "--wait_until_suspend";
  public static final String DB_CONFIG_FILE_OPTION_STR = "--db_config_file";

  public static final String RESULT_VERIFICATION_SCRIPT = "dbus2_json_compare.py";

  public static final String CLIENT_RESULT_DIR = "integration-test/var/work/";

  public static final String TESTCASE_OPTION_STR = "--testcase";

  protected String _ddsBaseDir;

  protected void loadSystemProperties()
  {
    Properties props = System.getProperties();
    _ddsBaseDir = props.getProperty(DDS_PRJ_BASED_DIR_TOKEN,
                                        DEFAULT_DDS_PRJ_BASED_DIR);
  }
  
  public void runCommandLineTest(String testName)
  throws IOException, InterruptedException, TimeoutException
  {
    ExternalCommand cmd;
    ArrayList<String> arguments = new ArrayList<String>();
    arguments.add(TESTCASE_OPTION_STR);
    arguments.add(testName);
    String[] argArray = arguments.toArray(new String[0]);
    cmd =
        ExternalCommand.executeWithTimeout(new File(_ddsBaseDir
            + INTEGRATION_SCRIPT_DIR), DRIVER_SCRIPT_NAME, EXEC_TIMEOUT, argArray);
    int exitValue = cmd.exitValue();

    if (0 != exitValue)
    {
//      String errString = new String(cmd.getError());
      String errString = new String(cmd.getOutput());
      AssertJUnit.assertTrue(errString, false);
    }
  }
}
