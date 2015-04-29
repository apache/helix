/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix Get information for zookeeper path
 * using http://localhost:8100/zkPath/{ClusterName}
 *
 */

var Helix = (function(Helix) {

    Helix.HelixZookeeperPathController = function($scope, $http) {

    	//listing clusters
        $http.get("http://localhost:8100/clusters")
            .success(function(data) {
                $scope.clusters = data;
                $scope.resource = null;
            })        

        
        //getting path from zkPath (zookeeper path)
        $scope.showPath = function(cluster) {
            $http.get("http://localhost:8100/zkPath/" + cluster.clusterName)
                .success(function(data) {
                    $scope.data = data;
                })
        }  
    };


    return Helix;

})(Helix || {});