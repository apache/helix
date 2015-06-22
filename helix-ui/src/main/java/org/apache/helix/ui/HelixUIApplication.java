package org.apache.helix.ui;

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

import com.google.common.collect.ImmutableMap;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import org.apache.helix.ui.health.ClusterConnectionHealthCheck;
import org.apache.helix.ui.resource.AdminResource;
import org.apache.helix.ui.resource.DashboardResource;
import org.apache.helix.ui.resource.VisualizerResource;
import org.apache.helix.ui.task.ClearClientCache;
import org.apache.helix.ui.task.ClearDataCacheTask;
import org.apache.helix.ui.util.ClientCache;
import org.apache.helix.ui.util.DataCache;
import org.apache.helix.ui.util.ZkAddressValidator;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;

public class HelixUIApplication extends Application<HelixUIApplicationConfiguration> {
  @Override
  public String getName() {
    return "helix-ui";
  }

  @Override
  public void initialize(Bootstrap<HelixUIApplicationConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle<HelixUIApplicationConfiguration>() {
      @Override
      public ImmutableMap<String, ImmutableMap<String, String>> getViewConfiguration(HelixUIApplicationConfiguration config) {
        return config.getViewRendererConfiguration();
      }
    });
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
    bootstrap.addBundle(new AssetsBundle("/assets/fonts", "/assets/fonts", null, "fonts"));
  }

  @Override
  public void run(HelixUIApplicationConfiguration config, Environment environment) throws Exception {
    final ZkAddressValidator zkAddressValidator = new ZkAddressValidator(config.getZkAddresses());
    final ClientCache clientCache = new ClientCache(zkAddressValidator);

    // Close all connections when application stops
    environment.lifecycle().addLifeCycleListener(new AbstractLifeCycle.AbstractLifeCycleListener() {
      @Override
      public void lifeCycleStopping(LifeCycle event) {
        clientCache.invalidateAll();
      }
    });

    DataCache dataCache = new DataCache(clientCache);


    DashboardResource dashboardResource
            = new DashboardResource(clientCache, dataCache, config.isAdminMode());

    environment.healthChecks().register("clusterConnection", new ClusterConnectionHealthCheck(clientCache));
    environment.jersey().register(dashboardResource);
    environment.jersey().register(new VisualizerResource(clientCache, dataCache));
    environment.admin().addTask(new ClearDataCacheTask(dataCache));
    environment.admin().addTask(new ClearClientCache(clientCache));

    if (config.isAdminMode()) {
      environment.jersey().register(new AdminResource(clientCache, dataCache));
    }
  }

  public static void main(String[] args) throws Exception {
    new HelixUIApplication().run(args);
  }
}
