var app = angular.module("app", []);

app.controller("AppCtrl", function($http) {
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