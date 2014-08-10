/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix Controller module
 * 
 * Show controller information
 * Enable/disable cluster
 * 
 * using /clusters/{clusterName}/controller
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
                    $scope.dataColor = null;

                })
        }
		
        //Enable cluster controller
        $scope.enable = function(cluster) {
            
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/Controller",'jsonParameters={"command":"enableCluster","enabled":"true"}')
                .success(function(data) {
                    $scope.data = data;
                    $scope.dataColor = "success";
                })
        }
        
       //disable cluster       
       $scope.disable = function(cluster) {
            
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/Controller",'jsonParameters={"command":"enableCluster","enabled":"false"}')
                .success(function(data) {
                    $scope.data = data;
                    $scope.dataColor = "danger";

                })
        }

    };


    return Helix;

})(Helix || {});