/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix chartting
 *
 */

var Helix = (function(Helix) {

    Helix.HelixVisualizationController = function($scope, $http) {

        $http.get("http://localhost:8100/clusters")
            .success(function(data) {
                $scope.clusters = data;
               
            })

       
        //getInformation of resources /clusters/{clusterName}/resourceGroups/{resourceName}
        $scope.getInformation = function(cluster,resourceName) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups/"+resourceName)
                .success(function(data) {
                    $scope.resource = data;
                })
        }
        
        //list resources
        $scope.listResources = function(clusterName) {
            $http.get("http://localhost:8100/clusters/" + clusterName + "/resourceGroups")
                .success(function(data) {
                    $scope.resources = data;
                })
        }
        
        //list all instances 
        $scope.listInstances = function(clusterName) {
           
            $http.get("http://localhost:8100/clusters/" + clusterName + "/instances")
                .success(function(data) {
                    $scope.instances = data;
                })
        }



    };


    return Helix;

})(Helix || {});