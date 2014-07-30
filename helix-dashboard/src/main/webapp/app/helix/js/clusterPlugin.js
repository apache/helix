/**
 * @module Simple
 * @mail Simple
 *
 * The main entry point for the Simple module
 *
 */
var Simple = (function(Simple) {

  /**
   * @property pluginName
   * @type {string}
   *
   * The name of this plugin
   */
  Simple.pluginName = 'helix_cluster_plugin';

  /**
   * @property log
   * @type {Logging.Logger}
   *
   * This plugin's logger instance
   */
  Simple.log = Logger.get('helix_cluster_plugin');

  /**
   * @property contextPath
   * @type {string}
   *
   * The top level path of this plugin on the server
   *
   */
  Simple.contextPath = "/helix_cluster_plugin/";

  /**
   * @property templatePath
   * @type {string}
   *
   * The path to this plugin's partials
   */
  Simple.templatePath = Simple.contextPath + "plugin/html/";

  /**
   * @property module
   * @type {object}
   *
   * This plugin's angularjs module instance.  This plugin only
   * needs hawtioCore to run, which provides services like
   * workspace, viewRegistry and layoutFull used by the
   * run function
   */
  Simple.module = angular.module('helix_cluster_plugin', ['hawtioCore'])
      .config(function($routeProvider) {

        /**
         * Here we define the route for our plugin.  One note is
         * to avoid using 'otherwise', as hawtio has a handler
         * in place when a route doesn't match any routes that
         * routeProvider has been configured with.
         */
        $routeProvider.
            when('/helix_cluster_plugin', {
              templateUrl: Simple.templatePath + 'helixcluster.html'
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
  Simple.module.run(function(workspace, viewRegistry, layoutFull) {

    Simple.log.info(Simple.pluginName, " loaded");

	Core.addCSS(Helix.contextPath + "plugin/css/bootstrap.css");
    // tell the app to use the full layout, also could use layoutTree
    // to get the JMX tree or provide a URL to a custom layout
    viewRegistry["helix_cluster_plugin"] = layoutFull;

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
      id: "simple",
      content: "Simple",
      title: "Simple plugin loaded dynamically",
      isValid: function(workspace) { return true; },
      href: function() { return "#/simple_plugin"; },
      isActive: function(workspace) { return workspace.isLinkActive("simple_plugin"); }

    });

  });

  /**
   * @function SimpleController
   * @param $scope
   * @param jolokia
   *
   * The controller for simple.html, only requires the jolokia
   * service from hawtioCore
   *
   */
  Simple.controller("AppCtrl", function($http) {
    var app = this;
    $http.get("http://localhost:8100/clusters")
      .success(function(data) {
        app.clusters = data;
      })

		app.addClusters = function(cluster) {
        $http.post("http://localhost:8100/clusters", 'jsonParameters={"command":"addCluster","clusterName":"'+cluster.clusterName+'"}')
          .success(function(data) {
		   app.cluster.clusterName = null;
            app.clusters = data;
          })
    }
	
	app.listClusters = function() {
        $http.get("http://localhost:8100/clusters")
      .success(function(data) {
        app.clusters = data;
      })
    }
	
	    app.removeClusters = function(cluster) {
        $http.delete("http://localhost:8100/clusters/"+cluster.clusterName)
          .success(function(data) {
		   app.cluster.clusterName = null;
		   app.listClusters();
		   alert('Succesfull Deleted');
          })
    }
	
})

  return Simple;

})(Simple || {});

// tell the hawtio plugin loader about our plugin so it can be
// bootstrapped with the rest of angular
hawtioPluginLoader.addModule(Simple.pluginName);
