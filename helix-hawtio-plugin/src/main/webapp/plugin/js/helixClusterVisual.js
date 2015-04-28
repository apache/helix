/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix simple visual board
 *
 */
 
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