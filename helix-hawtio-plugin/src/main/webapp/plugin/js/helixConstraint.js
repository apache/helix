/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix Constraints module
 * list/remove and add Constraints
 *
 */

<<<<<<< HEAD
=======
 /*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

>>>>>>> d2626146ef74db5b8d2ac5e79224923d2f8574e5
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
        
        
        //Set a contraint
        //curl -d 'jsonParameters={"constraintAttributes":"RESOURCE=MyDB,CONSTRAINT_VALUE=1"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/constraints/MESSAGE_CONSTRAINT/MyConstraint
          $scope.addConstraints = function(cluster) {          
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/constraints/"+cluster.constraintType+"/"+cluster.constraintName , 'jsonParameters={"constraintAttributes":"RESOURCE=' + cluster.resourceName + ',CONSTRAINT_VALUE='+cluster.constraintValue+'"}')
                .success(function(data) {
                    $scope.callback = cluster.constraintName + ' is added to '+cluster.clusterName;
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