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

    Helix.HelixControllerController = function($scope, $http) {

        $http.get("http://localhost:8100/clusters")
            .success(function(data) {
                $scope.clusters = data;
            })

        

        

        //list all info
        $scope.listInfo = function(cluster) {
            
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/Controller")
                .success(function(data) {
                    $scope.data = data;
                })
        }
		
		//list all instance's info
        $scope.listInstanceInfo = function(instance) {
            
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/instances/"+instance.id)
                .success(function(data) {
                    $scope.data = data;
                })
        }


    };


    return Helix;

})(Helix || {});