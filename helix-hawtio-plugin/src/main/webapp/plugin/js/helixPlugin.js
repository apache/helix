/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix module
 *
 */
<<<<<<< HEAD
=======
 
 /*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

>>>>>>> d2626146ef74db5b8d2ac5e79224923d2f8574e5
var Helix = (function(Helix) {

  /**
   * @property pluginName
   * @type {string}
   *
   * The name of this plugin
   */
  Helix.pluginName = 'helix_plugin';

  /**
   * @property log
   * @type {Logging.Logger}
   *
   * This plugin's logger instance
   */
  Helix.log = Logger.get('Helix');

  /**
   * @property contextPath
   * @type {string}
   *
   * The top level path of this plugin on the server
   *
   */
  Helix.contextPath = "/helix-plugin/";
  Helix.endPoint= "http://localhost:8100/clusters";

  /**
   * @property templatePath
   * @type {string}
   *
   * The path to this plugin's partials
   */
  Helix.templatePath = Helix.contextPath + "plugin/html/";

  /**
   * @property module
   * @type {object}
   *
   * This plugin's angularjs module instance.  This plugin only
   * needs hawtioCore to run, which provides services like
   * workspace, viewRegistry and layoutFull used by the
   * run function
   */
  Helix.module = angular.module('helix_plugin', ['hawtioCore'])
      .config(function($routeProvider) {

        /**
         * Here we define the route for our plugin.  One note is
         * to avoid using 'otherwise', as hawtio has a handler
         * in place when a route doesn't match any routes that
         * routeProvider has been configured with.
  		*/
			 $routeProvider
        .when('/helix_plugin', {
          templateUrl: Helix.templatePath + 'helix.html'
        })
        .when('/helix_plugin/cluster_manager', {
          templateUrl: Helix.templatePath + 'clusterManager.html'
        })
		.when('/helix_plugin/resource_manager', {
          templateUrl: Helix.templatePath + 'resourceManager.html'
        })
		.when('/helix_plugin/state_model_def_manager', {
          templateUrl: Helix.templatePath + 'stateModelDefManager.html'
        })
		.when('/helix_plugin/instance_manager', {
          templateUrl: Helix.templatePath + 'instanceManager.html'
        })
		.when('/helix_plugin/constraint_manager', {
          templateUrl: Helix.templatePath + 'constraintManager.html'
        })
		.when('/helix_plugin/config_manager', {
          templateUrl: Helix.templatePath + 'configManager.html'
        })
		.when('/helix_plugin/zookeeper_path_manager', {
          templateUrl: Helix.templatePath + 'zookeeperPathManager.html'
        })
		.when('/helix_plugin/controller_manager', {
          templateUrl: Helix.templatePath + 'controllerManager.html'
        })
		.when('/helix_plugin/cluster_visual_manager', {
          templateUrl: Helix.templatePath + 'clusterVisualManager.html'
        });
			
      });

  /**
   * Here we define any initialization to be done when this angular
   * module is bootstrapped.  In here we do a number of things:
   *
   * 1.  We log that we've been loaded (kinda optional)
   * 2.  We load our .css file for our views
   * 3.  We configure the viewRegistry service from hawtio for our
   *     route; in this case we use a pre-defined layout that uses
   *     the full viewing area
   * 4.  We configure our top-level tab and provide a link to our
   *     plugin.  This is just a matter of adding to the workspace's
   *     topLevelTabs array.
   */
  Helix.module.run(function(workspace, viewRegistry, layoutFull) {

    Helix.log.info(Helix.pluginName, " loaded");

    Core.addCSS(Helix.contextPath + "plugin/css/helix.css");
    Core.addCSS(Helix.contextPath + "plugin/css/bootstrap.css");

    // viewRegistry["helix_plugin"] = layoutFull;
    // tell hawtio that we have our own custom layout for
    // our view for helix 
    viewRegistry["helix_plugin"] = Helix.templatePath + "helixLayout.html";
    /* Set up top-level link to our plugin.  Requires an object
       with the following attributes:

         id - the ID of this plugin, used by the perspective plugin
              and by the preferences page
         content - The text or HTML that should be shown in the tab
         title - This will be the tab's tooltip
         isValid - A function that returns whether or not this
                   plugin has functionality that can be used for
                   the current JVM.  The workspace object is passed
                   in by hawtio's navbar controller which lets
                   you inspect the JMX tree, however you can do
                   any checking necessary and return a boolean
         href - a function that returns a link, normally you'd
                return a hash link like #/foo/bar but you can
                also return a full URL to some other site
         isActive - Called by hawtio's navbar to see if the current
                    $location.url() matches up with this plugin.
                    Here we use a helper from workspace that
                    checks if $location.url() starts with our
                    route.
     */
    workspace.topLevelTabs.push({
      id: "helix",
      content: "Helix",
      title: "Helix plugin loaded dynamically",
      isValid: function(workspace) { return true; },
      href: function() { return "#/helix_plugin"; },
      isActive: function(workspace) { return workspace.isLinkActive("helix_plugin"); }

    });

  });

  /**
   * @function HelixController
   * @param $scope
   * @param jolokia
   *
   * The controller for helix.html, only requires the jolokia
   * service from hawtioCore
   *
   */
  Helix.HelixController = function($scope,$http) {
    $scope.hello = "This is";
	$scope.dashboardName = "Helix Dashboard";
	//view for scope only
	$scope.HelixRestEndPoint = "http://localhost:8100/clusters";
	$http.get(Helix.endPoint)
      .success(function(data) {
        $scope.clusters = data;
        
      })



	//Add A New Cluster
	$scope.addClusters = function(cluster) {
        $http.post(Helix.endPoint, 'jsonParameters={"command":"addCluster","clusterName":"'+cluster.clusterName+'"}')
          .success(function(data) {
        	  $scope.cluster.clusterName = null;
        	  $scope.clusters = data;
          })
    }
	
	
 
	 //List the Cluster Names	 
	 $scope.listClusters =function() {
	        $http.get(Helix.endPoint)
	      .success(function(data) {
	    	  $scope.clusters = data;
	      })
	    }
		
	 //Remove A Cluster
	    $scope.removeClusters = function(clusters,clusterName) {
	        $http.delete(Helix.endPoint+"/"+clusterName)
	          .success(function(data) {
	        	  $scope.callback = clusterName+", is removed";
			   $scope.clusters.clusterName = null;
			   $scope.listClusters();
			   
	          })
	    }
		   
		   //Enable a Cluster in Controller mode
			  
		   $scope.enableCluster = function(cluster,clusterName) {
			   $http.post(Helix.endPoint+"/"+clusterName, 'jsonParameters={"command":"activateCluster","grandCluster":"'+cluster.controllerCluster+'","enabled":"true"}')
		          .success(function(data) {
		        	  $scope.callback = clusterName+", is enabled";
		        	 
		          })
		    }
		   
		   
		   //Disable a Cluster in Controller mode
		   
		   $scope.disableCluster = function(cluster,clusterName) {
			   $http.post(Helix.endPoint+"/"+clusterName, 'jsonParameters={"command":"activateCluster","grandCluster":"'+cluster.controllerCluster+'","enabled":"false"}')
		          .success(function(data) {		        	  
		           $scope.callback = clusterName+", is disabled";
		        	 
		          })
		    }
		   
		   //List Clusters' Information		   
		   $scope.listClusterInfo =function(clusterName) {
		        $http.get(Helix.endPoint+"/"+clusterName)
		      .success(function(data) {
		    	  $scope.callback = clusterName+", cluster information is listed in below";
		    	  $scope.clustersInfo = data;
		      })
		    }
  };
  

  return Helix;

})(Helix || {});

// tell the hawtio plugin loader about our plugin so it can be
// bootstrapped with the rest of angular
hawtioPluginLoader.addModule(Helix.pluginName);
