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

    Helix.HelixCintrollerController = function($scope, $http) {

        $http.get("http://localhost:8100/clusters")
            .success(function(data) {
                $scope.clusters = data;
            })

        

        

        //list all info
        $scope.listInfo = function(cluster) {
            
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/controller")
                .success(function(data) {
                    $scope.controller = data;
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