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
<<<<<<< HEAD
=======

>>>>>>> d2626146ef74db5b8d2ac5e79224923d2f8574e5
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
        
<<<<<<< HEAD
       //disable cluster
       
=======
       //disable cluster       
>>>>>>> d2626146ef74db5b8d2ac5e79224923d2f8574e5
       $scope.disable = function(cluster) {
            
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/Controller",'jsonParameters={"command":"enableCluster","enabled":"false"}')
                .success(function(data) {
                    $scope.data = data;
                    $scope.dataColor = "danger";
<<<<<<< HEAD
=======

>>>>>>> d2626146ef74db5b8d2ac5e79224923d2f8574e5
                })
        }

    };


    return Helix;

})(Helix || {});