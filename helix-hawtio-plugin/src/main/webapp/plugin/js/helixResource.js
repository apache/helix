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
                $scope.resource = null;
            })

        //adding resources
        $scope.addResources = function(cluster) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups", 'jsonParameters={"command":"addResource","resourceGroupName":"' + cluster.resourceName + '","partitions":"8","stateModelDefRef":"MasterSlave"}')
                .success(function(data) {
                    $scope.cluster.clusterName = null;
                    // $scope.clusters = data;
                    $scope.listResources(cluster);
                })
        }

        //lisitng clusters
        $scope.listClusters = function() {
            $http.get("http://localhost:8100/clusters")
                .success(function(data) {
                    $scope.clusters = data;
                    $scope.resource = null;
                })
        }
        
        //removing resources
        $scope.removeResource = function(cluster,resourceName) {
            $http.delete("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups/"+resourceName)
                .success(function(data) {
                    $scope.clusters = data;
                    $scope.resource = null;
                })
        }
        
        //getInformation of resources /clusters/{clusterName}/resourceGroups/{resourceName}
        $scope.getInformation = function(cluster,resourceName) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups/"+resourceName)
                .success(function(data) {
                    $scope.resourceInfor = data;
                })
        }
        //list resources
        $scope.listResources = function(cluster) {
            console.log(cluster.clusterName)
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups")
                .success(function(data) {
                    $scope.resources = data;
                    $scope.resource = null;
                })
        }
        //get view of mapping in external view of resources
        $scope.externalView = function(cluster,resourceName) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups/"+resourceName+"/externalView")
                .success(function(data) {
                    $scope.resourceInfor = data;
                })
        }

        //Reset all erroneous partitions of a resource
        $scope.reset = function(cluster,resourceName) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups/"+resourceName,'jsonParameters={"command":"resetResource"}')
                .success(function(data) {
                	$scope.callback =  ' was reset for all erroneous partitions in '+resourceName+ ' in '+ cluster.clusterName +'.'
                    $scope.resourceInfor = data;
                })
        }
        
        //Rebalance a resource
        $scope.rebalance = function(cluster,resourceName) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups/"+resourceName,'jsonParameters={"command":"resetResource","replicas":"'+ cluster.rebalanceNo +'"}')
                .success(function(data) {
                	$scope.callback =  ' Rebalance the partitions in '+resourceName+ ' in '+ cluster.clusterName +'.'
                    $scope.resourceInfor = data;
                })
        }


    };


    return Helix;

})(Helix || {});