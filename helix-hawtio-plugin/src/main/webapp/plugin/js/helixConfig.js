/**
 * @module Helix
 * @mail Helix
 *
 * The main entry point for the Helix config
 *
 */

var Helix = (function(Helix) {

    Helix.HelixConfigController = function($scope, $http) {

        $http.get("http://localhost:8100/clusters")
            .success(function(data) {
                $scope.clusters = data;
               
            })

        
		
		 //getting User Cluster Level Config 
        $scope.getClusterLevel = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/cluster")
                .success(function(data) {
                    $scope.configData = data;
                })
        }  
		
		//getting User Participant Level Config 
        //eg: http://localhost:8100/clusters/HELIX_QUICKSTART/configs/participant/localhost_12002
        $scope.getParticipantLevel = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/participant/"+cluster.instancesName.id)
                .success(function(data) {
                    $scope.configData = data;
                })
        }  
		
		//getting Resource Level Config 
        $scope.getResourceLevel = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/resource/"+cluster.resourcesName)
                .success(function(data) {
                    $scope.configData = data;
                })
        }  
		
		//setting User Cluster Level Config 
        $scope.setClusterLevel = function(cluster) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/cluster",'jsonParameters={"command":"setConfig","configs":"'+cluster.configKey+'='+cluster.configValue+'"}')
                .success(function(data) {
                    $scope.configData = data;
                })
        }  
		
		//setting User Participant Level Config 
        $scope.setParticipantLevel = function(cluster,instance) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/pariticipant/"+cluster.instancesName.id,'jsonParameters={"command":"setConfig","configs":"'+cluster.configKey+'='+cluster.configValue+'"}')
                .success(function(data) {
                    $scope.configData = data;
                })
        }  
		
		//setting Resource Level Config 
        $scope.setResourceLevel = function(cluster) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/resource/"+cluster.resourcesName,'jsonParameters={"command":"setConfig","configs":"'+cluster.configKey+'='+cluster.configValue+'"}')
                .success(function(data) {
                    $scope.configData = data;
                })
        }  
		
		//delete User Cluster Level Config 
        $scope.removeClusterLevel = function(cluster,key) {
            $http.post("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/cluster",'jsonParameters={"command":"removeConfig","configs":"'+key+'"}')
                .success(function(data) {
                    $scope.configData = data;
                })
        }  
		
		//delete User Participant Level Config 
        $scope.removeParticipantLevel = function(cluster,instance) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/pariticipant")
                .success(function(data) {
                    $scope.configData = data;
                })
        }  
		
		//deleteg Resource Level Config 
        $scope.removeResourceLevel = function(cluster,key) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/resource/"+cluster.resourcesName,'jsonParameters={"command":"removeConfig","configs":"'+key+'"}')
                .success(function(data) {
                    $scope.configData = data;
                })
        }  

        $scope.listClusters = function() {
            $http.get("http://localhost:8100/clusters")
                .success(function(data) {
                    $scope.clusters = data;
                    $scope.resource = null;
                })
        }
        
        //when user select level of condif this method will occur 
        $scope.update = function(cluster) {
        	$scope.participantSelect = false;
        	$scope.resourceSelect = false;
        	if(cluster.configLevel == "participant"){
        		$scope.participantSelect = true;
        		$scope.listInstances(cluster);
        		}
        	if(cluster.configLevel == "resource"){
            	$scope.resourceSelect = true;
            	$scope.listResources(cluster);
            	}
           
        }
        
        //list resources
        $scope.listResources = function(cluster) {
        	$scope.callback = null;
            console.log(cluster.clusterName)
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName + "/resourceGroups")
                .success(function(data) {
                    $scope.resources = data;
                   // $scope.resource = null;
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
        
        
        $scope.go = function(cluster,instance) {
		
		var configLevel = cluster.configLevel		
		var configAction = cluster.configAction
		switch(configLevel)
		{
			case "cluster":
			{
				
				switch(configAction)
				{
					case "get":
					$scope.getClusterLevel(cluster);
					break;
					
					case "set":
					$scope.setClusterLevel(cluster);
					break;
					
				
				}
				
			
			}
			break;
			
			case "participant":
			{
				
				switch(configAction)
				{
					case "get":
					$scope.getParticipantLevel(cluster);
					break;
					
					case "set":
					$scope.setParticipantLevel(cluster);
					break;
					
				
				}
				
			
			}
			 break;
			 
			
			case "resource":
			{
			
				switch(configAction)
				{
					case "get":
					$scope.getResourceLevel(cluster);
					break;
					
					case "set":
					$scope.setResourceLevel(cluster);
					break;
					
				
				}
		
			}
			break;
		}
            
        }//end of the go
        



    };


    return Helix;

})(Helix || {});