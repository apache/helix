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
                $scope.resource = null;
            })

        
		
		 //getting User Cluster Level Config 
        $scope.getClusterLevel = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/cluster")
                .success(function(data) {
                    $scope.data = data;
                })
        }  
		
		//getting User Participant Level Config 
        $scope.getParticipantLevel = function(cluster,instance) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/pariticipant/"+instance.instanceName)
                .success(function(data) {
                    $scope.data = data;
                })
        }  
		
		//getting Resource Level Config 
        $scope.getResourceLevel = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/resource")
                .success(function(data) {
                    $scope.data = data;
                })
        }  
		
		//setting User Cluster Level Config 
        $scope.setClusterLevel = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/cluster")
                .success(function(data) {
                    $scope.data = data;
                })
        }  
		
		//setting User Participant Level Config 
        $scope.setParticipantLevel = function(cluster,instance) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/pariticipant")
                .success(function(data) {
                    $scope.data = data;
                })
        }  
		
		//setting Resource Level Config 
        $scope.setResourceLevel = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/resource")
                .success(function(data) {
                    $scope.data = data;
                })
        }  
		
		//delete User Cluster Level Config 
        $scope.removeClusterLevel = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/cluster")
                .success(function(data) {
                    $scope.data = data;
                })
        }  
		
		//delete User Participant Level Config 
        $scope.removeParticipantLevel = function(cluster,instance) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/pariticipant")
                .success(function(data) {
                    $scope.data = data;
                })
        }  
		
		//deleteg Resource Level Config 
        $scope.removeResourceLevel = function(cluster) {
            $http.get("http://localhost:8100/clusters/" + cluster.clusterName+"/configs/resource")
                .success(function(data) {
                    $scope.data = data;
                })
        }  

        $scope.listClusters = function() {
            $http.get("http://localhost:8100/clusters")
                .success(function(data) {
                    $scope.clusters = data;
                    $scope.resource = null;
                })
        }
        
        
        
        $scope.go = function(cluster,instance) {
		
		var e = document.getElementById("level");
		var l =e.options[e.selectedIndex].value;
		
		var c = document.getElementById("action");
		var a=c.options[c.selectedIndex].value;
		
		switch(l)
		{
			case "cluster":
			{
				
				switch(a)
				{
					case "get":
					$scope.getClusterLevel(cluster);
					break;
					
					case "set":
					$scope.setClusterLevel(cluster);
					break;
					
					case "remove":
					$scope.removeClusterLevel(cluster);
					break;
				
				}
				
			
			}
			break;
			
			case "participant":
			{
				
				switch(a)
				{
					case "get":
					$scope.getParticipantLevel(cluster,instance);
					break;
					
					case "set":
					$scope.setParticipantLevel(cluster,instance);
					break;
					
					case "remove":
					$scope.removeParticipantLevel(cluster,instance);
					break;
				
				}
				
			
			}
			 break;
			 
			
			case "resource":
			{
			
				switch(a)
				{
					case "get":
					$scope.getResourceLevel(cluster);
					break;
					
					case "set":
					$scope.setResourceLevel(cluster);
					break;
					
					case "remove":
					$scope.removeResourceLevel(cluster);
					break;
				
				}
		
		
		
		
			}
			break;
		}
            
        }
        



    };


    return Helix;

})(Helix || {});