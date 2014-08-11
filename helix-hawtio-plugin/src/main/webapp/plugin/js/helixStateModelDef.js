/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix state model module
 *
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
                    $scope.statemodel = data;
                })
        }
        
        //get state model  definition
        $scope.getDefinition = function(cluster,stateModel) {
            $http.get(Helix.endPoint+"/" + cluster.clusterName + "/StateModelDefs/"+stateModel)
                .success(function(data) {
                    $scope.statemodelDefinition = data;
                })
        }
        
        // Add a state mdoel definition
        $scope.addStateModelDef = function(cluster,stateModelName) { 
        	$scope.callbackErr = null;
        	$scope.callback = null;
        	if(cluster.clusterName.length<1){
        		$scope.callbackErr = "Please select cluster name";
        		}else{
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
                	$scope.callback = 'State Mdoel definition, 'cluster.newStateModelDefinition + ' is added to '+cluster.clusterName;
                    $scope.statemodelDefinition = data;
                })
        `}

        }

    };


    return Helix;

})(Helix || {});