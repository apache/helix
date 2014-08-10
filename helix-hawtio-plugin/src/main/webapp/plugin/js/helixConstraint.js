/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix Constraints module
 * list/remove and add Constraints
 *
 */

var Helix = (function(Helix) {

    Helix.HelixConstraintController = function($scope, $http) {

        $http.get("http://localhost:8100/clusters")
            .success(function(data) {
                $scope.clusters = data;
                $scope.resource = null;
            })



        $scope.listClusters = function() {
            $http.get("http://localhost:8100/clusters")
                .success(function(data) {
                    $scope.clusters = data;
                    $scope.resource = null;
                })
        }
        

        
        //Show all contraints using  /clusters/{clusterName}/constraints/{constraintType}
        $scope.listConstraints = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/constraints/"+cluster.constraintType)
                .success(function(data) {
                    $scope.constraints = data;
                })
        }
        
        //DELETE constraints by /clusters/MyCluster/constraints/MESSAGE_CONSTRAINT/MyConstraint
        $scope.removeConstraint = function(cluster,constraintName) {          
            $http.delete("http://localhost:8100/clusters/" + cluster.clusterName + "/constraints/"+cluster.constraintType+"/"+constraintName)
                .success(function(data) {
                    $scope.callback = constraintName + ' in cluster "'+cluster.clusterName+ '" is successfully removed ';
                    $scope.listConstraints(cluster);
                })
                .error(function(data, status, headers, config) {
                	console.log(data);
                	log.info(data);
                  });

        }
        
        
          $scope.addConstraints = function(cluster) {          
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/constraints/"+cluster.constraintType,)
                .success(function(data) {
                    $scope.callback = constraintName + ' in cluster "'+cluster.clusterName+ '" is successfully removed ';
                    $scope.listConstraints(cluster);
                })
                .error(function(data, status, headers, config) {
                	console.log(data);
                	log.info(data);
                  });

        }


    };


    return Helix;

})(Helix || {});