/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix Resource Controllermodule
 * can list all resources in a cluster by calling /clusters/{clusterName}/resourceGroups
 * able to add a resource to cluster 
 *
 */
var Helix = (function(Helix) {

    Helix.HelixResourceController = function($scope, $http) {

        $http.get("http://localhost:8100/clusters")
            .success(function(data) {
                $scope.clusters = data;
            })

        //Add a resource to cluster    
        $scope.addResources = function(cluster) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups", 'jsonParameters={"command":"addResource","resourceGroupName":"' + cluster.resourceName + '","partitions":"8","stateModelDefRef":"MasterSlave"}')
                .success(function(data) {
                    // $scope.cluster.clusterName = null;
                    // $scope.clusters = data;
                    $scope.listResources(cluster);
                })
        }

        //List all clusters
        $scope.listClusters = function() {
            $http.get("http://localhost:8100/clusters")
                .success(function(data) {
                    $scope.clusters = data;
                })
        }

        //List all resources in a cluster
        $scope.listResources = function(cluster) {
            console.log(cluster.clusterName)
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups")
                .success(function(data) {
                    $scope.resources = data;
                })
        }


    return Helix;

})(Helix || {});