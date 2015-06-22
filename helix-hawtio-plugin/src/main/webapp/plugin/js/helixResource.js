/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix Resource Controllermodule
 * can list all resources in a cluster by calling /clusters/{clusterName}/resourceGroups
 * able to add a resource to cluster
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
                	$scope.callback = null;
                    // $scope.clusters = data;
                    $scope.listResources(cluster);
                })
        }

        //lisitng clusters
        $scope.listClusters = function() {
            $http.get("http://localhost:8100/clusters")
                .success(function(data) {
                	$scope.callback = null;
                    $scope.clusters = data;
                    $scope.resource = null;
                })
        }
        
        //removing resources
        $scope.removeResource = function(cluster,resourceName) {
            $http.delete("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups/"+resourceName)
                .success(function(data) {
                	$scope.callback = resourceName+ ' is removed';
                    $scope.clusters = data;
                    $scope.resource = null;
                })
        }
        
        //getInformation of resources /clusters/{clusterName}/resourceGroups/{resourceName}
        $scope.getInformation = function(cluster,resourceName) {
        	$scope.callback = null;
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups/"+resourceName)
                .success(function(data) {
                    $scope.resourceInfor = data;
                })
        }
        //list resources
        $scope.listResources = function(cluster) {
        	$scope.callback = null;
            console.log(cluster.clusterName)
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups")
                .success(function(data) {
                    $scope.resources = data;
                  //  $scope.resource = null;
                })
        }
        //get view of mapping in external view of resources
        $scope.externalView = function(cluster,resourceName) {
        	$scope.callback = null;
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
        
        $scope.addProperty = function(cluster,resourceName) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups/"+resourceName+"/idealState",'jsonParameters={"command":"addResourceProperty","'+cluster.resourcePropertyName+'":"'+cluster.resourcePropertyValue+'"}')
                .success(function(data) {
                	$scope.callback = cluster.resourcePropertyName+ ' Property is added to '+resourceName+ ' in '+ cluster.clusterName +'.'
                    $scope.resourceInfor = data;
                })
        }


    };


    return Helix;

})(Helix || {});