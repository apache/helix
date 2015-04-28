/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix state model module
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

    Helix.HelixStateModelDefController = function($scope, $http) {

        $http.get(Helix.endPoint)
            .success(function(data) {
                $scope.clusters = data;
            })

        //list clusters
        $scope.listClusters = function() {
            $http.get(Helix.endPoint)
                .success(function(data) {
                    $scope.clusters = data;
                })
        }
        

        //Show all state model 
        $scope.listStateModel = function(cluster) {
            $http.get(Helix.endPoint+"/" + cluster.clusterName + "/StateModelDefs")
                .success(function(data) {
                	//$scope.callback = null;
                    $scope.statemodel = data;
                })
        }
        
        //get state model  definition
        $scope.getDefinition = function(cluster,stateModel) {
            $http.get(Helix.endPoint+"/" + cluster.clusterName + "/StateModelDefs/"+stateModel)
                .success(function(data) {
                	$scope.callback = null;
                    $scope.statemodelDefinition = data;
                })
        }
        
        $scope.addStateModelDef  = function(cluster,stateModel) {
        	$scope.callback = null;
        	var hData = 'jsonParameters={"command":"addStateModelDef"'
            	+'}&newStateModelDef={'
            	  +'"id" : "'+cluster.newStateModelDefinition+'",'
            	  +'"simpleFields" : {"INITIAL_STATE" : "OFFLINE"},'
            	  +'"listFields" : {"STATE_PRIORITY_LIST" : [ "ONLINE", "OFFLINE", "DROPPED" ],"STATE_TRANSITION_PRIORITYLIST" : [ "OFFLINE-ONLINE", "ONLINE-OFFLINE", "OFFLINE-DROPPED" ] },'
            	  +'"mapFields" : {"DROPPED.meta" : { "count" : "-1" },'
            	  +'"OFFLINE.meta" : { "count" : "-1" },'
            	  +'"OFFLINE.next" : { "DROPPED" : "DROPPED","ONLINE" : "ONLINE" },'
            	  +'"ONLINE.meta" : { "count" : "R" },'
            	  +'"ONLINE.next" : { "DROPPED" : "OFFLINE","OFFLINE" : "OFFLINE"'
            	  +'}'
            	  +'}'
            	  +'}';
        	 $http.post(Helix.endPoint+"/" + cluster.clusterName + "/StateModelDefs",hData)
                .success(function(data) {
                	$scope.callback = 'State Mdoel definition, '+cluster.newStateModelDefinition + ' is added to '+cluster.clusterName;
                	$scope.statemodelDefinition = data;
                	$scope.listStateModel(cluster);
                })
        }


    };


    return Helix;

})(Helix || {});