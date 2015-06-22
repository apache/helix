/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix Instance Controller module
 * 
 * can list all instances /clusters/{clusterName}/instances
 * able add an instance
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

    Helix.HelixInstanceController = function($scope, $http) {

        $http.get(Helix.endPoint)
            .success(function(data) {
                $scope.clusters = data;
            })

        //add an instance   
        $scope.addInstances = function(cluster) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/instances", 'jsonParameters={"command":"addInstance","instanceNames":"' + cluster.instanceName + '"}')
                .success(function(data) {
                    //$scope.cluster.clusterName = null;
                    // $scope.clusters = data;
                    $scope.listInstances(cluster);
                })
        }

        

        //list all instances 
        $scope.listInstances = function(cluster) {
            console.log(cluster.clusterName)
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/instances")
                .success(function(data) {
                    $scope.instances = data;
                })
        }
		
		//list all instance's info
        $scope.listInstanceInfo = function(cluster,instanceId) {            
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/instances/"+instanceId)
                .success(function(data) {
                    $scope.instancesInfor = data;
                })
        }
        
        //droup instaces
        $scope.removeInstance = function(cluster,instanceId) {            
            $http.delete("http://localhost:8100/clusters/" + cluster.clusterName + "/instances/"+instanceId)
                .success(function(data) {
                	if(data.ERROR){
                		$scope.callback = null;
                		$scope.callbackErr =  instanceId + ' can not drop'
                	}else{
                	$scope.callback =  instanceId + ' is removed'
                	$scope.callbackErr = null;
                    $scope.listInstances(cluster);}
                })
        }
        
		//disable an instance
        $scope.disableInstance = function(cluster,instanceId) {            
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/instances/"+instanceId,'jsonParameters={"command":"enableInstance","enabled":"false"}')
                .success(function(data) {
                	$scope.callback =  instanceId + ' is disabled'
                    $scope.instancesInfor = data;
                })
        }
        
		//enable an instance
        $scope.enableInstance = function(cluster,instanceId) {            
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/instances/"+instanceId,'jsonParameters={"command":"enableInstance","enabled":"true"}')
                .success(function(data) {
                	$scope.callback =  instanceId + ' is enabled'
                    $scope.instancesInfor = data;
                })
        }
        
        //resetInstance
        $scope.resetInstance = function(cluster,instanceId) {            
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName + "/instances/"+instanceId,'jsonParameters={"command":"resetInstance"}')
                .success(function(data) {
                	$scope.callback =  instanceId + ' is reseted'
                    $scope.instancesInfor = data;
                })
        }


    };


    return Helix;

})(Helix || {});