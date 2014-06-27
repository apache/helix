package org.apache.helix.webapp;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.logging.Level;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.webapp.resources.ClusterResource;
import org.apache.helix.webapp.resources.ClustersResource;
import org.apache.helix.webapp.resources.ConfigResource;
import org.apache.helix.webapp.resources.ConstraintResource;
import org.apache.helix.webapp.resources.ControllerResource;
import org.apache.helix.webapp.resources.ControllerStatusUpdateResource;
import org.apache.helix.webapp.resources.CurrentStateResource;
import org.apache.helix.webapp.resources.CurrentStatesResource;
import org.apache.helix.webapp.resources.ErrorResource;
import org.apache.helix.webapp.resources.ErrorsResource;
import org.apache.helix.webapp.resources.ExternalViewResource;
import org.apache.helix.webapp.resources.IdealStateResource;
import org.apache.helix.webapp.resources.InstanceResource;
import org.apache.helix.webapp.resources.InstancesResource;
import org.apache.helix.webapp.resources.ResourceGroupResource;
import org.apache.helix.webapp.resources.ResourceGroupsResource;
import org.apache.helix.webapp.resources.SchedulerTasksResource;
import org.apache.helix.webapp.resources.StateModelResource;
import org.apache.helix.webapp.resources.StateModelsResource;
import org.apache.helix.webapp.resources.StatusUpdateResource;
import org.apache.helix.webapp.resources.StatusUpdatesResource;
import org.apache.helix.webapp.resources.WorkflowsResource;
import org.apache.helix.webapp.resources.ZkChildResource;
import org.apache.helix.webapp.resources.ZkPathResource;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.data.MediaType;
import org.restlet.representation.StringRepresentation;
import org.restlet.routing.Router;
import org.restlet.routing.Template;

public class RestAdminApplication extends Application {
  public static final String HELP = "help";
  public static final String ZKSERVERADDRESS = "zkSvr";
  public static final String PORT = "port";
  public static final String ZKCLIENT = "zkClient";
  public static final int DEFAULT_PORT = 8100;

  static {
    org.restlet.engine.Engine.setLogLevel(Level.SEVERE);
  }

  public RestAdminApplication() {
    super();
  }

  public RestAdminApplication(Context context) {
    super(context);
  }

  @Override
  public Restlet createInboundRoot() {
    Router router = new Router(getContext());
    router.setDefaultMatchingMode(Template.MODE_EQUALS);
    router.attach("/clusters", ClustersResource.class);
    router.attach("/clusters/{clusterName}", ClusterResource.class);
    router.attach("/clusters/{clusterName}/resourceGroups", ResourceGroupsResource.class);
    router.attach("/clusters/{clusterName}/resourceGroups/{resourceName}",
        ResourceGroupResource.class);
    router.attach("/clusters/{clusterName}/workflows", WorkflowsResource.class);
    router.attach("/clusters/{clusterName}/instances", InstancesResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}", InstanceResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/currentState/{resourceName}",
        CurrentStateResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/statusUpdate/{resourceName}",
        StatusUpdateResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/errors/{resourceName}",
        ErrorResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/currentState",
        CurrentStatesResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/statusUpdate",
        StatusUpdatesResource.class);
    router.attach("/clusters/{clusterName}/instances/{instanceName}/errors", ErrorsResource.class);
    router.attach("/clusters/{clusterName}/resourceGroups/{resourceName}/idealState",
        IdealStateResource.class);
    router.attach("/clusters/{clusterName}/resourceGroups/{resourceName}/externalView",
        ExternalViewResource.class);
    router.attach("/clusters/{clusterName}/StateModelDefs/{modelName}", StateModelResource.class);
    router.attach("/clusters/{clusterName}/StateModelDefs", StateModelsResource.class);
    router.attach("/clusters/{clusterName}/SchedulerTasks", SchedulerTasksResource.class);
    router.attach("/clusters/{clusterName}/Controller", ControllerResource.class);
    router.attach("/clusters/{clusterName}/Controller/statusUpdates/{MessageType}/{MessageId}",
        ControllerStatusUpdateResource.class);
    router.attach("/clusters/{clusterName}/configs", ConfigResource.class);
    router.attach("/clusters/{clusterName}/configs/{scope}", ConfigResource.class);
    router.attach("/clusters/{clusterName}/configs/{scope}/{scopeKey1}", ConfigResource.class);
    router.attach("/clusters/{clusterName}/configs/{scope}/{scopeKey1}/{scopeKey2}",
        ConfigResource.class);
    router.attach("/clusters/{clusterName}/constraints/{constraintType}", ConstraintResource.class);
    router.attach("/clusters/{clusterName}/constraints/{constraintType}/{constraintId}",
        ConstraintResource.class);
    router.attach("/zkPath", ZkPathResource.class).setMatchingMode(Template.MODE_STARTS_WITH);
    router.attach("/zkChild", ZkChildResource.class).setMatchingMode(Template.MODE_STARTS_WITH);

    Restlet mainpage = new Restlet() {
      @Override
      public void handle(Request request, Response response) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("<html>");
        stringBuilder.append("<head><title>Restlet Cluster Management page</title></head>");
        stringBuilder.append("<body bgcolor=white>");
        stringBuilder.append("<table border=\"0\">");
        stringBuilder.append("<tr>");
        stringBuilder.append("<td>");
        stringBuilder.append("<h1>Rest cluster management interface V1</h1>");
        stringBuilder.append("</td>");
        stringBuilder.append("</tr>");
        stringBuilder.append("</table>");
        stringBuilder.append("</body>");
        stringBuilder.append("</html>");
        response.setEntity(new StringRepresentation(stringBuilder.toString(), MediaType.TEXT_HTML));
      }
    };
    router.attach("", mainpage);
    return router;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + RestAdminApplication.class.getName(), cliOptions);
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(HELP).withDescription("Prints command-line options info")
            .create();
    helpOption.setArgs(0);
    helpOption.setRequired(false);
    helpOption.setArgName("print help message");

    Option zkServerOption =
        OptionBuilder.withLongOpt(ZKSERVERADDRESS).withDescription("Provide zookeeper address")
            .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option portOption =
        OptionBuilder.withLongOpt(PORT).withDescription("Provide web service port").create();
    portOption.setArgs(1);
    portOption.setRequired(false);
    portOption.setArgName("web service port, default: " + DEFAULT_PORT);

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(portOption);

    return options;
  }

  public static void processCommandLineArgs(String[] cliArgs) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try {
      cmd = cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      System.err.println("RestAdminApplication: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    int port = DEFAULT_PORT;
    if (cmd.hasOption(HELP)) {
      printUsage(cliOptions);
      return;
    } else if (cmd.hasOption(PORT)) {
      port = Integer.parseInt(cmd.getOptionValue(PORT));
    }

    HelixAdminWebApp app = new HelixAdminWebApp(cmd.getOptionValue(ZKSERVERADDRESS), port);
    app.start();
    try {
      Thread.currentThread().join();
    } finally {
      app.stop();
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    processCommandLineArgs(args);

  }

}
