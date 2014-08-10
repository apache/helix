/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix Instance Controller module
 * 
 * can list all instances /clusters/{clusterName}/instances
 * able add an instance
 *
 */
var Helix = (function(Helix) {

    Helix.HelixInstanceController = function($scope, $http) {

        $http.get("http://localhost:8100/clusters")
            .success(function(data) {
                $scope.clusters = data;
            })

        //add an instance   
        $scope.addInstances = function(cluster) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/instances", 'jsonParameters={"command":"addInstance","instanceNames":"' + cluster.instanceName + '"}')
                .success(function(data) {
                    //$scope.cluster.clusterName = null;
                    // $scope.clusters = data;
                    $scope.listInstances(cluster);
                })
        }

        

        //list all instances 
        $scope.listInstances = function(cluster) {
            console.log(cluster.clusterName)
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/instances")
                .success(function(data) {
                    $scope.instances = data;
                })
        }
		
		//list all instance's info
        $scope.listInstanceInfo = function(instance) {
            
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/instances/"+instance.id)
                .success(function(data) {
                    $scope.instances = data;
                })
        }


    };


    return Helix;

})(Helix || {});